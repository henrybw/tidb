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

package executor

import (
	"context"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
)

const (
	defaultRetrieveCount = 1024
)

func buildStmtSummaryRetriever(
	table *model.TableInfo,
	columns []*model.ColumnInfo,
	extractor *plannercore.StatementsSummaryExtractor,
) memTableRetriever {
	if extractor == nil {
		extractor = &plannercore.StatementsSummaryExtractor{}
	}
	if extractor.Digests.Empty() {
		extractor.Digests = nil
	}

	var retriever memTableRetriever
	if extractor.SkipRequest {
		retriever = &dummyRetriever{}
	} else {
		retriever = &stmtSummaryRetriever{
			table:   table,
			columns: columns,
			digests: extractor.Digests,
		}
	}

	return retriever
}

type dummyRetriever struct {
	dummyCloser
}

func (*dummyRetriever) retrieve(_ context.Context, _ sessionctx.Context) ([][]types.Datum, error) {
	return nil, nil
}

// stmtSummaryRetriever is used to retrieve statements summary.
type stmtSummaryRetriever struct {
	table   *model.TableInfo
	columns []*model.ColumnInfo
	digests set.StringSet

	// lazily initialized
	rowsReader *rowsReader
}

func (e *stmtSummaryRetriever) retrieve(_ context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if err := e.ensureRowsReader(sctx); err != nil {
		return nil, err
	}
	return e.rowsReader.read(defaultRetrieveCount)
}

func (e *stmtSummaryRetriever) close() error {
	if e.rowsReader != nil {
		return e.rowsReader.close()
	}
	return nil
}

func (*stmtSummaryRetriever) getRuntimeStats() execdetails.RuntimeStats {
	return nil
}

func (e *stmtSummaryRetriever) ensureRowsReader(sctx sessionctx.Context) error {
	if e.rowsReader != nil {
		return nil
	}

	var err error
	if isEvictedTable(e.table.Name.O) {
		e.rowsReader, err = e.initEvictedRowsReader(sctx)
	} else {
		e.rowsReader, err = e.initSummaryRowsReader(sctx)
	}

	return err
}

func (e *stmtSummaryRetriever) initEvictedRowsReader(sctx sessionctx.Context) (*rowsReader, error) {
	if err := checkPrivilege(sctx); err != nil {
		return nil, err
	}

	rows := stmtsummary.StmtSummaryByDigestMap.ToEvictedCountDatum()
	if !isClusterTable(e.table.Name.O) {
		// rows are full-columned, so we need to adjust them to the required columns.
		return newSimpleRowsReader(adjustColumns(rows, e.columns, e.table)), nil
	}

	// Additional column `INSTANCE` for cluster table
	rows, err := infoschema.AppendHostInfoToRows(sctx, rows)
	if err != nil {
		return nil, err
	}
	// rows are full-columned, so we need to adjust them to the required columns.
	return newSimpleRowsReader(adjustColumns(rows, e.columns, e.table)), nil
}

func (e *stmtSummaryRetriever) initSummaryRowsReader(sctx sessionctx.Context) (*rowsReader, error) {
	vars := sctx.GetSessionVars()
	user := vars.User
	tz := vars.StmtCtx.TimeZone()
	columns := e.columns
	priv := hasPriv(sctx, mysql.ProcessPriv)
	instanceAddr, err := clusterTableInstanceAddr(sctx, e.table.Name.O)
	if err != nil {
		return nil, err
	}

	reader := stmtsummary.NewStmtSummaryReader(user, priv, columns, instanceAddr, tz)
	if e.digests != nil {
		// set checker to filter out statements not matching the given digests
		checker := stmtsummary.NewStmtSummaryChecker(e.digests)
		reader.SetChecker(checker)
	}

	var rows [][]types.Datum
	if isCumulativeTable(e.table.Name.O) {
		rows = reader.GetStmtSummaryCumulativeRows()
	} else if isCurrentTable(e.table.Name.O) {
		rows = reader.GetStmtSummaryCurrentRows()
	} else if isHistoryTable(e.table.Name.O) {
		rows = reader.GetStmtSummaryHistoryRows()
	}
	return newSimpleRowsReader(rows), nil
}

type rowsReader struct {
	rows [][]types.Datum
}

func newSimpleRowsReader(rows [][]types.Datum) *rowsReader {
	return &rowsReader{rows: rows}
}

func (r *rowsReader) read(maxCount int) ([][]types.Datum, error) {
	if maxCount >= len(r.rows) {
		ret := r.rows
		r.rows = nil
		return ret, nil
	}
	ret := r.rows[:maxCount]
	r.rows = r.rows[maxCount:]
	return ret, nil
}

func (*rowsReader) close() error {
	return nil
}

func isClusterTable(originalTableName string) bool {
	switch originalTableName {
	case infoschema.ClusterTableStatementsSummary,
		infoschema.ClusterTableStatementsSummaryHistory,
		infoschema.ClusterTableStatementsSummaryEvicted,
		infoschema.ClusterTableTiDBStatementsStats:
		return true
	}

	return false
}

func isCumulativeTable(originalTableName string) bool {
	switch originalTableName {
	case infoschema.TableTiDBStatementsStats,
		infoschema.ClusterTableTiDBStatementsStats:
		return true
	}

	return false
}

func isCurrentTable(originalTableName string) bool {
	switch originalTableName {
	case infoschema.TableStatementsSummary,
		infoschema.ClusterTableStatementsSummary:
		return true
	}

	return false
}

func isHistoryTable(originalTableName string) bool {
	switch originalTableName {
	case infoschema.TableStatementsSummaryHistory,
		infoschema.ClusterTableStatementsSummaryHistory:
		return true
	}

	return false
}

func isEvictedTable(originalTableName string) bool {
	switch originalTableName {
	case infoschema.TableStatementsSummaryEvicted,
		infoschema.ClusterTableStatementsSummaryEvicted:
		return true
	}

	return false
}

func checkPrivilege(sctx sessionctx.Context) error {
	if !hasPriv(sctx, mysql.ProcessPriv) {
		return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
	}
	return nil
}

func clusterTableInstanceAddr(sctx sessionctx.Context, originalTableName string) (string, error) {
	if isClusterTable(originalTableName) {
		return infoschema.GetInstanceAddr(sctx)
	}
	return "", nil
}
