// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importer

import (
	"context"
	"io"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// constants, make it a variable for test
var (
	maxKVQueueSize         = 32             // Cache at most this number of rows before blocking the encode loop
	MinDeliverBytes uint64 = 96 * units.KiB // 96 KB (data + index). batch at least this amount of bytes to reduce number of messages
	// MinDeliverRowCnt see default for tikv-importer.max-kv-pairs
	MinDeliverRowCnt = 4096
)

type rowToEncode struct {
	row   []types.Datum
	rowID int64
	// endOffset represents the offset after the current row in encode reader.
	// it will be negative if the data source is not file.
	endOffset int64
	resetFn   func()
}

type encodeReaderFn func(ctx context.Context, row []types.Datum) (data rowToEncode, closed bool, err error)

// parserEncodeReader wraps a mydump.Parser as a encodeReaderFn.
func parserEncodeReader(parser mydump.Parser, endOffset int64, filename string) encodeReaderFn {
	return func(context.Context, []types.Datum) (data rowToEncode, closed bool, err error) {
		readPos, _ := parser.Pos()
		if readPos >= endOffset {
			closed = true
			return
		}

		err = parser.ReadRow()
		// todo: we can implement a ScannedPos which don't return error, will change it later.
		currOffset, _ := parser.ScannedPos()
		switch errors.Cause(err) {
		case nil:
		case io.EOF:
			closed = true
			err = nil
			return
		default:
			err = common.ErrEncodeKV.Wrap(err).GenWithStackByArgs(filename, currOffset)
			return
		}
		lastRow := parser.LastRow()
		data = rowToEncode{
			row:       lastRow.Row,
			rowID:     lastRow.RowID,
			endOffset: currOffset,
			resetFn:   func() { parser.RecycleRow(lastRow) },
		}
		return
	}
}

type queryChunkEncodeReader struct {
	chunkCh <-chan QueryChunk
	currChk QueryChunk
	cursor  int
	numRows int
}

func (r *queryChunkEncodeReader) readRow(ctx context.Context, row []types.Datum) (data rowToEncode, closed bool, err error) {
	if r.currChk.Chk == nil || r.cursor >= r.numRows {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case currChk, ok := <-r.chunkCh:
			if !ok {
				closed = true
				return
			}
			r.currChk = currChk
			r.cursor = 0
			r.numRows = r.currChk.Chk.NumRows()
		}
	}

	chkRow := r.currChk.Chk.GetRow(r.cursor)
	rowLen := chkRow.Len()
	if cap(row) < rowLen {
		row = make([]types.Datum, rowLen)
	} else {
		row = row[:0]
		for range rowLen {
			row = append(row, types.Datum{}) // nozero
		}
	}
	row = chkRow.GetDatumRowWithBuffer(r.currChk.Fields, row)
	r.cursor++
	data = rowToEncode{
		row:       row,
		rowID:     r.currChk.RowIDOffset + int64(r.cursor),
		endOffset: -1,
		resetFn:   func() {},
	}
	return
}

// queryRowEncodeReader wraps a queryChunkEncodeReader as a encodeReaderFn.
func queryRowEncodeReader(chunkCh <-chan QueryChunk) encodeReaderFn {
	reader := queryChunkEncodeReader{chunkCh: chunkCh}
	return func(ctx context.Context, row []types.Datum) (data rowToEncode, closed bool, err error) {
		return reader.readRow(ctx, row)
	}
}

type encodedKVGroupBatch struct {
	dataKVs  []common.KvPair
	indexKVs map[int64][]common.KvPair // indexID -> pairs

	bytesBuf *kv.BytesBuf
	memBuf   *kv.MemBuf

	groupChecksum *verify.KVGroupChecksum
}

func (b *encodedKVGroupBatch) reset() {
	if b.memBuf == nil {
		return
	}
	// mimic kv.Pairs.Clear
	b.memBuf.Recycle(b.bytesBuf)
	b.bytesBuf = nil
	b.memBuf = nil
}

func newEncodedKVGroupBatch(keyspace []byte, count int) *encodedKVGroupBatch {
	return &encodedKVGroupBatch{
		dataKVs:       make([]common.KvPair, 0, count),
		indexKVs:      make(map[int64][]common.KvPair, 8),
		groupChecksum: verify.NewKVGroupChecksumWithKeyspace(keyspace),
	}
}

// add must be called with `kvs` from the same session for a encodedKVGroupBatch.
func (b *encodedKVGroupBatch) add(kvs *kv.Pairs) error {
	for _, pair := range kvs.Pairs {
		if tablecodec.IsRecordKey(pair.Key) {
			b.dataKVs = append(b.dataKVs, pair)
			b.groupChecksum.UpdateOneDataKV(pair)
		} else {
			indexID, err := tablecodec.DecodeIndexID(pair.Key)
			if err != nil {
				return errors.Trace(err)
			}
			if len(b.indexKVs[indexID]) == 0 {
				b.indexKVs[indexID] = make([]common.KvPair, 0, cap(b.dataKVs))
			}
			b.indexKVs[indexID] = append(b.indexKVs[indexID], pair)
			b.groupChecksum.UpdateOneIndexKV(indexID, pair)
		}
	}

	// the related buf is shared, so we only need to record any one of them.
	if b.bytesBuf == nil && kvs.BytesBuf != nil {
		b.bytesBuf = kvs.BytesBuf
		b.memBuf = kvs.MemBuf
	}
	return nil
}

// chunkEncoder encodes data from readFn and sends encoded data to sendFn.
type chunkEncoder struct {
	readFn    encodeReaderFn
	offset    int64
	sendFn    func(ctx context.Context, batch *encodedKVGroupBatch) error
	collector execute.Collector

	chunkName string
	logger    *zap.Logger
	encoder   *TableKVEncoder
	keyspace  []byte

	// total duration takes by read/encode.
	readTotalDur   time.Duration
	encodeTotalDur time.Duration

	groupChecksum *verify.KVGroupChecksum
}

func newChunkEncoder(
	chunkName string,
	readFn encodeReaderFn,
	offset int64,
	sendFn func(ctx context.Context, batch *encodedKVGroupBatch) error,
	collector execute.Collector,
	logger *zap.Logger,
	encoder *TableKVEncoder,
	keyspace []byte,
) *chunkEncoder {
	return &chunkEncoder{
		chunkName:     chunkName,
		readFn:        readFn,
		offset:        offset,
		sendFn:        sendFn,
		collector:     collector,
		logger:        logger,
		encoder:       encoder,
		keyspace:      keyspace,
		groupChecksum: verify.NewKVGroupChecksumWithKeyspace(keyspace),
	}
}

func (p *chunkEncoder) encodeLoop(ctx context.Context) error {
	var (
		encodedBytesCounter, encodedRowsCounter prometheus.Counter
		readDur, encodeDur                      time.Duration
		rowCount                                int
		rowBatch                                = make([]*kv.Pairs, 0, MinDeliverRowCnt)
		rowBatchByteSize                        uint64
		currOffset                              int64
	)
	metrics, _ := metric.GetCommonMetric(ctx)
	if metrics != nil {
		encodedBytesCounter = metrics.BytesCounter.WithLabelValues(metric.StateRestored)
		// table name doesn't matter here, all those metrics will have task-id label.
		encodedRowsCounter = metrics.RowsCounter.WithLabelValues(metric.StateRestored, "")
	}

	recordSendReset := func() error {
		if len(rowBatch) == 0 {
			return nil
		}

		var delta int64
		if currOffset >= 0 {
			delta = currOffset - p.offset
			p.offset = currOffset
			if metrics != nil {
				// if we're using split_file, this metric might larger than total
				// source file size, as the offset we're using is the reader offset,
				// not parser offset, and we'll buffer data.
				encodedBytesCounter.Add(float64(delta))
			}
		}

		if metrics != nil {
			metrics.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())
			metrics.RowReadSecondsHistogram.Observe(readDur.Seconds())
			encodedRowsCounter.Add(float64(rowCount))
		}
		p.encodeTotalDur += encodeDur
		p.readTotalDur += readDur

		kvGroupBatch := newEncodedKVGroupBatch(p.keyspace, rowCount)
		for _, kvs := range rowBatch {
			if err := kvGroupBatch.add(kvs); err != nil {
				return errors.Trace(err)
			}
		}

		p.groupChecksum.Add(kvGroupBatch.groupChecksum)

		if err := p.sendFn(ctx, kvGroupBatch); err != nil {
			return err
		}

		if p.collector != nil {
			p.collector.Add(delta, int64(rowCount))
		}

		// the ownership of rowBatch is transferred to the receiver of sendFn, we should
		// not touch it anymore.
		rowBatch = make([]*kv.Pairs, 0, MinDeliverRowCnt)
		rowBatchByteSize = 0
		rowCount = 0
		readDur = 0
		encodeDur = 0
		return nil
	}

	var readRowCache []types.Datum
	for {
		readDurStart := time.Now()
		data, closed, err := p.readFn(ctx, readRowCache)
		if err != nil {
			return errors.Trace(err)
		}
		if closed {
			break
		}
		readRowCache = data.row
		readDur += time.Since(readDurStart)

		encodeDurStart := time.Now()
		kvs, encodeErr := p.encoder.Encode(data.row, data.rowID)
		currOffset = data.endOffset
		data.resetFn()
		if encodeErr != nil {
			// todo: record and ignore encode error if user set max-errors param
			return common.ErrEncodeKV.Wrap(encodeErr).GenWithStackByArgs(p.chunkName, data.endOffset)
		}
		encodeDur += time.Since(encodeDurStart)

		rowCount++
		rowBatch = append(rowBatch, kvs)
		rowBatchByteSize += kvs.Size()
		// pebble cannot allow > 4.0G kv in one batch.
		// we will meet pebble panic when import sql file and each kv has the size larger than 4G / maxKvPairsCnt.
		// so add this check.
		if rowBatchByteSize >= MinDeliverBytes || len(rowBatch) >= MinDeliverRowCnt {
			if err := recordSendReset(); err != nil {
				return err
			}
		}
	}

	return recordSendReset()
}

func (p *chunkEncoder) summaryFields() []zap.Field {
	mergedChecksum := p.groupChecksum.MergedChecksum()
	return []zap.Field{
		zap.Duration("readDur", p.readTotalDur),
		zap.Duration("encodeDur", p.encodeTotalDur),
		zap.Object("checksum", &mergedChecksum),
	}
}

// ChunkProcessor is used to process a chunk of data, include encode data to KV
// and deliver KV to local or global storage.
type ChunkProcessor interface {
	Process(ctx context.Context) error
}

type baseChunkProcessor struct {
	sourceType    DataSourceType
	enc           *chunkEncoder
	deliver       *dataDeliver
	logger        *zap.Logger
	groupChecksum *verify.KVGroupChecksum
}

func (p *baseChunkProcessor) Process(ctx context.Context) (err error) {
	task := log.BeginTask(p.logger, "process chunk")
	defer func() {
		logFields := append(p.enc.summaryFields(), p.deliver.summaryFields()...)
		logFields = append(logFields, zap.Stringer("type", p.sourceType))
		task.End(zap.ErrorLevel, err, logFields...)
		if metrics, ok := metric.GetCommonMetric(ctx); ok && err == nil {
			metrics.ChunkCounter.WithLabelValues(metric.ChunkStateFinished).Inc()
		}
	}()

	group, gCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	group.Go(func() error {
		return p.deliver.deliverLoop(gCtx)
	})
	group.Go(func() error {
		defer p.deliver.encodeDone()
		return p.enc.encodeLoop(gCtx)
	})

	err2 := group.Wait()
	// in some unit tests it's nil
	if c := p.groupChecksum; c != nil {
		c.Add(p.enc.groupChecksum)
	}
	return err2
}

// NewFileChunkProcessor creates a new local sort chunk processor.
// exported for test.
func NewFileChunkProcessor(
	parser mydump.Parser,
	encoder *TableKVEncoder,
	keyspace []byte,
	chunk *checkpoints.ChunkCheckpoint,
	logger *zap.Logger,
	diskQuotaLock *syncutil.RWMutex,
	dataWriter backend.EngineWriter,
	indexWriter backend.EngineWriter,
	groupChecksum *verify.KVGroupChecksum,
	collector execute.Collector,
) ChunkProcessor {
	chunkLogger := logger.With(zap.String("key", chunk.GetKey()))
	deliver := &dataDeliver{
		logger:        chunkLogger,
		diskQuotaLock: diskQuotaLock,
		kvBatch:       make(chan *encodedKVGroupBatch, maxKVQueueSize),
		dataWriter:    dataWriter,
		indexWriter:   indexWriter,
	}
	return &baseChunkProcessor{
		sourceType: DataSourceTypeFile,
		deliver:    deliver,
		enc: newChunkEncoder(
			chunk.GetKey(),
			parserEncodeReader(parser, chunk.Chunk.EndOffset, chunk.GetKey()),
			chunk.Chunk.Offset,
			deliver.sendEncodedData,
			collector,
			chunkLogger,
			encoder,
			keyspace,
		),
		logger:        chunkLogger,
		groupChecksum: groupChecksum,
	}
}

type dataDeliver struct {
	logger        *zap.Logger
	kvBatch       chan *encodedKVGroupBatch
	diskQuotaLock *syncutil.RWMutex
	dataWriter    backend.EngineWriter
	indexWriter   backend.EngineWriter

	deliverTotalDur time.Duration
}

func (p *dataDeliver) encodeDone() {
	close(p.kvBatch)
}

func (p *dataDeliver) sendEncodedData(ctx context.Context, batch *encodedKVGroupBatch) error {
	select {
	case p.kvBatch <- batch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *dataDeliver) deliverLoop(ctx context.Context) error {
	var (
		dataKVBytesHist, indexKVBytesHist prometheus.Observer
		dataKVPairsHist, indexKVPairsHist prometheus.Observer
		deliverBytesCounter               prometheus.Counter
	)

	metrics, _ := metric.GetCommonMetric(ctx)
	if metrics != nil {
		dataKVBytesHist = metrics.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindData)
		indexKVBytesHist = metrics.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindIndex)
		dataKVPairsHist = metrics.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindData)
		indexKVPairsHist = metrics.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindIndex)
		deliverBytesCounter = metrics.BytesCounter.WithLabelValues(metric.StateRestoreWritten)
	}

	for {
		var (
			kvBatch *encodedKVGroupBatch
			ok      bool
		)

		select {
		case kvBatch, ok = <-p.kvBatch:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		err := func() error {
			p.diskQuotaLock.RLock()
			defer p.diskQuotaLock.RUnlock()

			start := time.Now()
			if err := p.dataWriter.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvBatch.dataKVs)); err != nil {
				if !common.IsContextCanceledError(err) {
					p.logger.Error("write to data engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}
			if err := p.indexWriter.AppendRows(ctx, nil, kv.GroupedPairs(kvBatch.indexKVs)); err != nil {
				if !common.IsContextCanceledError(err) {
					p.logger.Error("write to index engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}

			deliverDur := time.Since(start)
			p.deliverTotalDur += deliverDur
			if metrics != nil {
				metrics.BlockDeliverSecondsHistogram.Observe(deliverDur.Seconds())

				dataSize, indexSize := kvBatch.groupChecksum.DataAndIndexSumSize()
				dataKVCnt, indexKVCnt := kvBatch.groupChecksum.DataAndIndexSumKVS()
				dataKVBytesHist.Observe(float64(dataSize))
				dataKVPairsHist.Observe(float64(dataKVCnt))
				indexKVBytesHist.Observe(float64(indexSize))
				indexKVPairsHist.Observe(float64(indexKVCnt))
				deliverBytesCounter.Add(float64(dataSize + indexSize))
			}
			return nil
		}()
		if err != nil {
			return err
		}

		kvBatch.reset()
	}
}

func (p *dataDeliver) summaryFields() []zap.Field {
	return []zap.Field{
		zap.Duration("deliverDur", p.deliverTotalDur),
	}
}

// QueryChunk is a chunk from query result.
type QueryChunk struct {
	Fields      []*types.FieldType
	Chk         *chunk.Chunk
	RowIDOffset int64
}

func newQueryChunkProcessor(
	chunkCh chan QueryChunk,
	encoder *TableKVEncoder,
	keyspace []byte,
	logger *zap.Logger,
	diskQuotaLock *syncutil.RWMutex,
	dataWriter backend.EngineWriter,
	indexWriter backend.EngineWriter,
	groupChecksum *verify.KVGroupChecksum,
	collector execute.Collector,
) ChunkProcessor {
	chunkName := "import-from-select"
	chunkLogger := logger.With(zap.String("key", chunkName))
	deliver := &dataDeliver{
		logger:        chunkLogger,
		diskQuotaLock: diskQuotaLock,
		kvBatch:       make(chan *encodedKVGroupBatch, maxKVQueueSize),
		dataWriter:    dataWriter,
		indexWriter:   indexWriter,
	}
	return &baseChunkProcessor{
		sourceType: DataSourceTypeQuery,
		deliver:    deliver,
		enc: newChunkEncoder(
			chunkName,
			queryRowEncodeReader(chunkCh),
			-1,
			deliver.sendEncodedData,
			collector,
			chunkLogger,
			encoder,
			keyspace,
		),
		logger:        chunkLogger,
		groupChecksum: groupChecksum,
	}
}

// WriterFactory is a factory function to create a new index KV writer.
type WriterFactory func(indexID int64) (*external.Writer, error)

// IndexRouteWriter is a writer for index when using global sort.
// we route kvs of different index to different writer in order to make
// merge sort easier, else kv data of all subtasks will all be overlapped.
type IndexRouteWriter struct {
	writers       map[int64]*external.Writer
	logger        *zap.Logger
	writerFactory WriterFactory
}

// NewIndexRouteWriter creates a new IndexRouteWriter.
func NewIndexRouteWriter(logger *zap.Logger, writerFactory WriterFactory) *IndexRouteWriter {
	return &IndexRouteWriter{
		writers:       make(map[int64]*external.Writer),
		logger:        logger,
		writerFactory: writerFactory,
	}
}

// AppendRows implements backend.EngineWriter interface.
func (w *IndexRouteWriter) AppendRows(ctx context.Context, _ []string, rows encode.Rows) error {
	groupedKvs, ok := rows.(kv.GroupedPairs)
	if !ok {
		return errors.Errorf("invalid kv pairs type for IndexRouteWriter: %T", rows)
	}
	for indexID, kvs := range groupedKvs {
		for _, item := range kvs {
			writer, ok := w.writers[indexID]
			if !ok {
				var err error
				writer, err = w.writerFactory(indexID)
				if err != nil {
					return errors.Trace(err)
				}
				w.writers[indexID] = writer
			}
			if err := writer.WriteRow(ctx, item.Key, item.Val, nil); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

// IsSynced implements backend.EngineWriter interface.
func (*IndexRouteWriter) IsSynced() bool {
	return true
}

// Close implements backend.EngineWriter interface.
func (w *IndexRouteWriter) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	var firstErr error
	for _, writer := range w.writers {
		if err := writer.Close(ctx); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			w.logger.Error("close index writer failed", zap.Error(err))
		}
	}
	return nil, firstErr
}
