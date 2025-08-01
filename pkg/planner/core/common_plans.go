// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/texttree"
	"github.com/pingcap/tipb/go-tipb"
)

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	baseSchemaProducer
}

// ShowSlow is for showing slow queries.
type ShowSlow struct {
	baseSchemaProducer

	*ast.ShowSlow
}

// ShowDDLJobQueries is for showing DDL job queries sql.
type ShowDDLJobQueries struct {
	baseSchemaProducer

	JobIDs []int64
}

// ShowDDLJobQueriesWithRange is for showing DDL job queries sql with specified limit and offset.
type ShowDDLJobQueriesWithRange struct {
	baseSchemaProducer

	Limit  uint64
	Offset uint64
}

// ShowNextRowID is for showing the next global row ID.
type ShowNextRowID struct {
	baseSchemaProducer
	TableName *ast.TableName
}

// CheckTable is used for checking table data, built from the 'admin check table' statement.
type CheckTable struct {
	baseSchemaProducer

	DBName             string
	Table              table.Table
	IndexInfos         []*model.IndexInfo
	IndexLookUpReaders []*PhysicalIndexLookUpReader
	CheckIndex         bool
}

// RecoverIndex is used for backfilling corrupted index data.
type RecoverIndex struct {
	baseSchemaProducer

	Table     *resolve.TableNameW
	IndexName string
}

// CleanupIndex is used to delete dangling index data.
type CleanupIndex struct {
	baseSchemaProducer

	Table     *resolve.TableNameW
	IndexName string
}

// CheckIndexRange is used for checking index data, output the index values that handle within begin and end.
type CheckIndexRange struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string

	HandleRanges []ast.HandleRange
}

// ChecksumTable is used for calculating table checksum, built from the `admin checksum table` statement.
type ChecksumTable struct {
	baseSchemaProducer

	Tables []*resolve.TableNameW
}

// CancelDDLJobs represents a cancel DDL jobs plan.
type CancelDDLJobs struct {
	baseSchemaProducer

	JobIDs []int64
}

// PauseDDLJobs indicates a plan to pause the Running DDL Jobs.
type PauseDDLJobs struct {
	baseSchemaProducer

	JobIDs []int64
}

// ResumeDDLJobs indicates a plan to resume the Paused DDL Jobs.
type ResumeDDLJobs struct {
	baseSchemaProducer

	JobIDs []int64
}

const (
	// AlterDDLJobThread alter reorg worker count
	AlterDDLJobThread = "thread"
	// AlterDDLJobBatchSize alter reorg batch size
	AlterDDLJobBatchSize = "batch_size"
	// AlterDDLJobMaxWriteSpeed alter reorg max write speed
	AlterDDLJobMaxWriteSpeed = "max_write_speed"
)

var allowedAlterDDLJobParams = map[string]struct{}{
	AlterDDLJobThread:        {},
	AlterDDLJobBatchSize:     {},
	AlterDDLJobMaxWriteSpeed: {},
}

// AlterDDLJobOpt represents alter ddl job option.
type AlterDDLJobOpt struct {
	Name  string
	Value expression.Expression
}

// AlterDDLJob is the plan of admin alter ddl job
type AlterDDLJob struct {
	baseSchemaProducer

	JobID   int64
	Options []*AlterDDLJobOpt
}

// WorkloadRepoCreate is the plan of admin create workload snapshot.
type WorkloadRepoCreate struct {
	baseSchemaProducer
}

// ReloadExprPushdownBlacklist reloads the data from expr_pushdown_blacklist table.
type ReloadExprPushdownBlacklist struct {
	baseSchemaProducer
}

// ReloadOptRuleBlacklist reloads the data from opt_rule_blacklist table.
type ReloadOptRuleBlacklist struct {
	baseSchemaProducer
}

// AdminPluginsAction indicate action will be taken on plugins.
type AdminPluginsAction int

const (
	// Enable indicates enable plugins.
	Enable AdminPluginsAction = iota + 1
	// Disable indicates disable plugins.
	Disable
)

// AdminPlugins administrates tidb plugins.
type AdminPlugins struct {
	baseSchemaProducer
	Action  AdminPluginsAction
	Plugins []string
}

// Change represents a change plan.
type Change struct {
	baseSchemaProducer
	*ast.ChangeStmt
}

// Prepare represents prepare plan.
type Prepare struct {
	baseSchemaProducer

	Name    string
	SQLText string
}

// Execute represents prepare plan.
type Execute struct {
	baseSchemaProducer

	Name     string
	Params   []expression.Expression
	PrepStmt *PlanCacheStmt
	Stmt     ast.StmtNode
	Plan     base.Plan
}

// Check if result of GetVar expr is BinaryLiteral
// Because GetVar use String to represent BinaryLiteral, here we need to convert string back to BinaryLiteral.
func isGetVarBinaryLiteral(sctx base.PlanContext, expr expression.Expression) (res bool) {
	scalarFunc, ok := expr.(*expression.ScalarFunction)
	if ok && scalarFunc.FuncName.L == ast.GetVar {
		name, isNull, err := scalarFunc.GetArgs()[0].EvalString(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err != nil || isNull {
			res = false
		} else if dt, ok2 := sctx.GetSessionVars().GetUserVarVal(name); ok2 {
			res = dt.Kind() == types.KindBinaryLiteral
		}
	}
	return res
}

// Deallocate represents deallocate plan.
type Deallocate struct {
	baseSchemaProducer

	Name string
}

// Set represents a plan for set stmt.
type Set struct {
	baseSchemaProducer

	VarAssigns []*expression.VarAssignment
}

// SetConfig represents a plan for set config stmt.
type SetConfig struct {
	baseSchemaProducer

	Type     string
	Instance string
	Name     string
	Value    expression.Expression
}

// RecommendIndexPlan represents a plan for recommend index stmt.
type RecommendIndexPlan struct {
	baseSchemaProducer

	Action   string
	SQL      string
	AdviseID int64
	Options  []ast.RecommendIndexOption
}

// SQLBindOpType repreents the SQL bind type
type SQLBindOpType int

const (
	// OpSQLBindCreate represents the operation to create a SQL bind.
	OpSQLBindCreate SQLBindOpType = iota
	// OpSQLBindDrop represents the operation to drop a SQL bind.
	OpSQLBindDrop
	// OpFlushBindings is used to flush plan bindings.
	OpFlushBindings
	// OpCaptureBindings is used to capture plan bindings.
	OpCaptureBindings
	// OpReloadBindings is used to reload plan binding.
	OpReloadBindings
	// OpSetBindingStatus is used to set binding status.
	OpSetBindingStatus
	// OpSQLBindDropByDigest is used to drop SQL binds by digest
	OpSQLBindDropByDigest
	// OpSetBindingStatusByDigest represents the operation to set SQL binding status by sql digest.
	OpSetBindingStatusByDigest
)

// SQLBindPlan represents a plan for SQL bind.
// One SQLBindPlan can be either global or session, and can only contain one type of operation, but can contain multiple
// operations of that type.
type SQLBindPlan struct {
	baseSchemaProducer

	IsGlobal  bool
	SQLBindOp SQLBindOpType
	Details   []*SQLBindOpDetail
}

// SQLBindOpDetail represents the detail of an operation on a single binding.
// Different SQLBindOpType use different fields in this struct.
type SQLBindOpDetail struct {
	NormdOrigSQL string
	BindSQL      string
	BindStmt     ast.StmtNode
	Db           string
	Charset      string
	Collation    string
	NewStatus    string
	Source       string // Source indicate how this binding was created, eg: bindinfo.Manual or bindinfo.History
	SQLDigest    string
	PlanDigest   string
}

// Simple represents a simple statement plan which doesn't need any optimization.
type Simple struct {
	baseSchemaProducer

	Statement ast.StmtNode

	// IsFromRemote indicates whether the statement IS FROM REMOTE TiDB instance in cluster,
	//   and executing in co-processor.
	//   Used for `global kill`. See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md.
	IsFromRemote bool

	// StaleTxnStartTS is the StartTS that is used to build a staleness transaction by 'START TRANSACTION READ ONLY' statement.
	StaleTxnStartTS uint64

	ResolveCtx *resolve.Context
}

// MemoryUsage return the memory usage of Simple
func (s *Simple) MemoryUsage() (sum int64) {
	if s == nil {
		return
	}

	sum = s.baseSchemaProducer.MemoryUsage() + size.SizeOfInterface + size.SizeOfBool + size.SizeOfUint64
	return
}

// PhysicalSimpleWrapper is a wrapper of `Simple` to implement physical plan interface.
//
//	Used for simple statements executing in coprocessor.
type PhysicalSimpleWrapper struct {
	physicalop.BasePhysicalPlan
	Inner Simple
}

// MemoryUsage return the memory usage of PhysicalSimpleWrapper
func (p *PhysicalSimpleWrapper) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + p.Inner.MemoryUsage()
	return
}

// InsertGeneratedColumns is for completing generated columns in Insert.
// We resolve generation expressions in plan, and eval those in executor.
type InsertGeneratedColumns struct {
	Exprs        []expression.Expression
	OnDuplicates []*expression.Assignment
}

func (i InsertGeneratedColumns) cloneForPlanCache() InsertGeneratedColumns {
	return InsertGeneratedColumns{
		Exprs:        cloneExpressionsForPlanCache(i.Exprs, nil),
		OnDuplicates: util.CloneAssignments(i.OnDuplicates),
	}
}

// MemoryUsage return the memory usage of InsertGeneratedColumns
func (i *InsertGeneratedColumns) MemoryUsage() (sum int64) {
	if i == nil {
		return
	}
	sum = size.SizeOfSlice*3 + int64(cap(i.OnDuplicates))*size.SizeOfPointer + int64(cap(i.Exprs))*size.SizeOfInterface

	for _, expr := range i.Exprs {
		sum += expr.MemoryUsage()
	}
	for _, as := range i.OnDuplicates {
		sum += as.MemoryUsage()
	}
	return
}

// Insert represents an insert plan.
type Insert struct {
	baseSchemaProducer

	Table         table.Table        `plan-cache-clone:"shallow"`
	tableSchema   *expression.Schema `plan-cache-clone:"shallow"`
	tableColNames types.NameSlice    `plan-cache-clone:"shallow"`
	Columns       []*ast.ColumnName  `plan-cache-clone:"shallow"`
	Lists         [][]expression.Expression

	OnDuplicate        []*expression.Assignment
	Schema4OnDuplicate *expression.Schema `plan-cache-clone:"shallow"`
	names4OnDuplicate  types.NameSlice    `plan-cache-clone:"shallow"`

	GenCols InsertGeneratedColumns

	SelectPlan base.PhysicalPlan

	IsReplace bool
	IgnoreErr bool

	// NeedFillDefaultValue is true when expr in value list reference other column.
	NeedFillDefaultValue bool

	AllAssignmentsAreConstant bool

	RowLen int

	FKChecks   []*FKCheck   `plan-cache-clone:"must-nil"`
	FKCascades []*FKCascade `plan-cache-clone:"must-nil"`
}

// MemoryUsage return the memory usage of Insert
func (p *Insert) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.baseSchemaProducer.MemoryUsage() + size.SizeOfInterface + size.SizeOfSlice*7 + int64(cap(p.tableColNames)+
		cap(p.Columns)+cap(p.OnDuplicate)+cap(p.names4OnDuplicate)+cap(p.FKChecks))*size.SizeOfPointer +
		p.GenCols.MemoryUsage() + size.SizeOfInterface + size.SizeOfBool*4 + size.SizeOfInt
	if p.tableSchema != nil {
		sum += p.tableSchema.MemoryUsage()
	}
	if p.Schema4OnDuplicate != nil {
		sum += p.Schema4OnDuplicate.MemoryUsage()
	}
	if p.SelectPlan != nil {
		sum += p.SelectPlan.MemoryUsage()
	}

	for _, name := range p.tableColNames {
		sum += name.MemoryUsage()
	}
	for _, exprs := range p.Lists {
		sum += size.SizeOfSlice + int64(cap(exprs))*size.SizeOfInterface
		for _, expr := range exprs {
			sum += expr.MemoryUsage()
		}
	}
	for _, as := range p.OnDuplicate {
		sum += as.MemoryUsage()
	}
	for _, name := range p.names4OnDuplicate {
		sum += name.MemoryUsage()
	}
	for _, fkC := range p.FKChecks {
		sum += fkC.MemoryUsage()
	}

	return
}

// Update represents Update plan.
type Update struct {
	baseSchemaProducer

	OrderedList []*expression.Assignment

	AllAssignmentsAreConstant bool

	IgnoreError bool

	VirtualAssignmentsOffset int

	SelectPlan base.PhysicalPlan

	// TblColPosInfos is for multi-table update statement.
	// It records the column position of each related table.
	TblColPosInfos TblColPosInfoSlice `plan-cache-clone:"shallow"`

	// Used when partition sets are given.
	// e.g. update t partition(p0) set a = 1;
	PartitionedTable []table.PartitionedTable `plan-cache-clone:"shallow"`

	// tblID2Table stores related tables' info of this Update statement.
	tblID2Table map[int64]table.Table `plan-cache-clone:"shallow"`

	FKChecks   map[int64][]*FKCheck   `plan-cache-clone:"must-nil"`
	FKCascades map[int64][]*FKCascade `plan-cache-clone:"must-nil"`
}

// MemoryUsage return the memory usage of Update
func (p *Update) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.baseSchemaProducer.MemoryUsage() + size.SizeOfSlice*3 + int64(cap(p.OrderedList))*size.SizeOfPointer +
		size.SizeOfBool + size.SizeOfInt + size.SizeOfInterface + int64(cap(p.PartitionedTable))*size.SizeOfInterface +
		int64(len(p.tblID2Table))*(size.SizeOfInt64+size.SizeOfInterface)
	if p.SelectPlan != nil {
		sum += p.SelectPlan.MemoryUsage()
	}

	for _, as := range p.OrderedList {
		sum += as.MemoryUsage()
	}
	for _, colInfo := range p.TblColPosInfos {
		sum += colInfo.MemoryUsage()
	}
	for _, v := range p.FKChecks {
		sum += size.SizeOfInt64 + size.SizeOfSlice + int64(cap(v))*size.SizeOfPointer
		for _, fkc := range v {
			sum += fkc.MemoryUsage()
		}
	}
	return
}

// Delete represents a delete plan.
type Delete struct {
	baseSchemaProducer

	IsMultiTable bool

	SelectPlan base.PhysicalPlan

	TblColPosInfos TblColPosInfoSlice `plan-cache-clone:"shallow"`

	FKChecks   map[int64][]*FKCheck   `plan-cache-clone:"must-nil"`
	FKCascades map[int64][]*FKCascade `plan-cache-clone:"must-nil"`

	IgnoreErr bool
}

// MemoryUsage return the memory usage of Delete
func (p *Delete) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.baseSchemaProducer.MemoryUsage() + size.SizeOfBool + size.SizeOfInterface + size.SizeOfSlice
	if p.SelectPlan != nil {
		sum += p.SelectPlan.MemoryUsage()
	}
	for _, colInfo := range p.TblColPosInfos {
		sum += colInfo.MemoryUsage()
	}
	return
}

// AnalyzeInfo is used to store the database name, table name and partition name of analyze task.
type AnalyzeInfo struct {
	DBName        string
	TableName     string
	PartitionName string
	TableID       statistics.AnalyzeTableID
	StatsVersion  int
	V2Options     *V2AnalyzeOptions
}

// V2AnalyzeOptions is used to hold analyze options information.
type V2AnalyzeOptions struct {
	PhyTableID  int64
	RawOpts     map[ast.AnalyzeOptionType]uint64
	FilledOpts  map[ast.AnalyzeOptionType]uint64
	ColChoice   ast.ColumnChoice
	ColumnList  []*model.ColumnInfo
	IsPartition bool
}

// AnalyzeColumnsTask is used for analyze columns.
type AnalyzeColumnsTask struct {
	HandleCols       util.HandleCols
	CommonHandleInfo *model.IndexInfo
	ColsInfo         []*model.ColumnInfo
	SkipColsInfo     []*model.ColumnInfo
	TblInfo          *model.TableInfo
	Indexes          []*model.IndexInfo
	AnalyzeInfo
}

// AnalyzeIndexTask is used for analyze index.
type AnalyzeIndexTask struct {
	IndexInfo *model.IndexInfo
	TblInfo   *model.TableInfo
	AnalyzeInfo
}

// Analyze represents an analyze plan
type Analyze struct {
	baseSchemaProducer

	ColTasks []AnalyzeColumnsTask
	IdxTasks []AnalyzeIndexTask
	Opts     map[ast.AnalyzeOptionType]uint64
	// OptionsMap is used to store the options for each partition.
	OptionsMap map[int64]V2AnalyzeOptions
}

// LoadData represents a loaddata plan.
type LoadData struct {
	baseSchemaProducer

	FileLocRef  ast.FileLocRefTp
	OnDuplicate ast.OnDuplicateKeyHandlingType
	Path        string
	Format      *string
	Table       *resolve.TableNameW
	Charset     *string
	Columns     []*ast.ColumnName
	FieldsInfo  *ast.FieldsClause
	LinesInfo   *ast.LinesClause
	IgnoreLines *uint64

	ColumnAssignments  []*ast.Assignment
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	Options            []*LoadDataOpt

	GenCols InsertGeneratedColumns
}

// LoadDataOpt represents load data option.
type LoadDataOpt struct {
	// Name is the name of the option, converted to lower case during parse.
	Name  string
	Value expression.Expression
}

// ImportInto represents a ingest into plan.
type ImportInto struct {
	baseSchemaProducer

	Table              *resolve.TableNameW
	ColumnAssignments  []*ast.Assignment
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	Path               string
	Format             *string
	Options            []*LoadDataOpt

	GenCols InsertGeneratedColumns
	Stmt    string

	SelectPlan base.PhysicalPlan
}

// LoadStats represents a load stats plan.
type LoadStats struct {
	baseSchemaProducer

	Path string
}

// LockStats represents a lock stats for table
type LockStats struct {
	baseSchemaProducer

	Tables []*ast.TableName
}

// UnlockStats represents a unlock stats for table
type UnlockStats struct {
	baseSchemaProducer

	Tables []*ast.TableName
}

// PlanReplayer represents a plan replayer plan.
type PlanReplayer struct {
	baseSchemaProducer
	ExecStmt          ast.StmtNode
	Analyze           bool
	Load              bool
	File              string
	HistoricalStatsTS uint64

	Capture    bool
	Remove     bool
	SQLDigest  string
	PlanDigest string
}

// Traffic represents a traffic plan.
type Traffic struct {
	baseSchemaProducer
	OpType  ast.TrafficOpType
	Options []*ast.TrafficOption
	Dir     string
}

// DistributeTable represents a distribute table plan.
type DistributeTable struct {
	baseSchemaProducer
	TableInfo      *model.TableInfo
	PartitionNames []ast.CIStr
	Engine         string
	Rule           string
	Timeout        string
}

// SplitRegion represents a split regions plan.
type SplitRegion struct {
	baseSchemaProducer

	TableInfo      *model.TableInfo
	PartitionNames []ast.CIStr
	IndexInfo      *model.IndexInfo
	Lower          []types.Datum
	Upper          []types.Datum
	Num            int
	ValueLists     [][]types.Datum
}

// SplitRegionStatus represents a split regions status plan.
type SplitRegionStatus struct {
	baseSchemaProducer

	Table     table.Table
	IndexInfo *model.IndexInfo
}

// CompactTable represents a "ALTER TABLE [NAME] COMPACT ..." plan.
type CompactTable struct {
	baseSchemaProducer

	ReplicaKind    ast.CompactReplicaKind
	TableInfo      *model.TableInfo
	PartitionNames []ast.CIStr
}

// DDL represents a DDL statement plan.
type DDL struct {
	baseSchemaProducer

	Statement ast.DDLNode
}

// SelectInto represents a select-into plan.
type SelectInto struct {
	baseSchemaProducer

	TargetPlan base.Plan
	IntoOpt    *ast.SelectIntoOption
	LineFieldsInfo
}

// LineFieldsInfo used in load-data/select-into/index-advise stmt.
type LineFieldsInfo struct {
	FieldsTerminatedBy string
	FieldsEnclosedBy   string // length always <= 1, see parser.y
	FieldsEscapedBy    string // length always <= 1, see parser.y
	FieldsOptEnclosed  bool
	LinesStartingBy    string
	LinesTerminatedBy  string
}

// NewLineFieldsInfo new LineFieldsInfo from FIELDS/LINES info.
func NewLineFieldsInfo(fieldsInfo *ast.FieldsClause, linesInfo *ast.LinesClause) LineFieldsInfo {
	e := LineFieldsInfo{
		FieldsTerminatedBy: "\t",
		FieldsEnclosedBy:   "",
		FieldsEscapedBy:    "\\",
		FieldsOptEnclosed:  false,
		LinesStartingBy:    "",
		LinesTerminatedBy:  "\n",
	}

	if fieldsInfo != nil {
		if fieldsInfo.Terminated != nil {
			e.FieldsTerminatedBy = *fieldsInfo.Terminated
		}
		if fieldsInfo.Enclosed != nil {
			e.FieldsEnclosedBy = *fieldsInfo.Enclosed
		}
		if fieldsInfo.Escaped != nil {
			e.FieldsEscapedBy = *fieldsInfo.Escaped
		}
		e.FieldsOptEnclosed = fieldsInfo.OptEnclosed
	}
	if linesInfo != nil {
		if linesInfo.Starting != nil {
			e.LinesStartingBy = *linesInfo.Starting
		}
		if linesInfo.Terminated != nil {
			e.LinesTerminatedBy = *linesInfo.Terminated
		}
	}
	return e
}

// ExplainInfoForEncode store explain info for JSON encode
type ExplainInfoForEncode struct {
	ID                  string                  `json:"id"`
	EstRows             string                  `json:"estRows"`
	ActRows             string                  `json:"actRows,omitempty"`
	TaskType            string                  `json:"taskType"`
	AccessObject        string                  `json:"accessObject,omitempty"`
	ExecuteInfo         string                  `json:"executeInfo,omitempty"`
	OperatorInfo        string                  `json:"operatorInfo,omitempty"`
	EstCost             string                  `json:"estCost,omitempty"`
	CostFormula         string                  `json:"costFormula,omitempty"`
	MemoryInfo          string                  `json:"memoryInfo,omitempty"`
	DiskInfo            string                  `json:"diskInfo,omitempty"`
	TotalMemoryConsumed string                  `json:"totalMemoryConsumed,omitempty"`
	SubOperators        []*ExplainInfoForEncode `json:"subOperators,omitempty"`
}

// JSONToString convert json to string
func JSONToString(j []*ExplainInfoForEncode) (string, error) {
	byteBuffer := bytes.NewBuffer([]byte{})
	encoder := json.NewEncoder(byteBuffer)
	// avoid wrongly embedding
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "    ")
	err := encoder.Encode(j)
	if err != nil {
		return "", err
	}
	return byteBuffer.String(), nil
}

// Explain represents a explain plan.
type Explain struct {
	baseSchemaProducer

	TargetPlan       base.Plan
	Format           string
	Analyze          bool
	Explore          bool   // EXPLAIN EXPLORE statement
	SQLDigest        string // "EXPLAIN EXPLORE <sql_digest>"
	ExecStmt         ast.StmtNode
	RuntimeStatsColl *execdetails.RuntimeStatsColl

	Rows            [][]string
	BriefBinaryPlan string
}

// GetBriefBinaryPlan returns the binary plan of the plan for explainfor.
func GetBriefBinaryPlan(p base.Plan) string {
	var plan base.Plan = p
	if plan == nil {
		return ""
	}
	// If the plan is a prepared statement, get execute.Plan.
	if exec, ok := p.(*Execute); ok {
		plan = exec.Plan
	}
	// Get plan context and statement context
	planCtx := plan.SCtx()
	if planCtx == nil {
		return ""
	}
	flat := FlattenPhysicalPlan(plan, true)
	return BinaryPlanStrFromFlatPlan(planCtx, flat, true)
}

// GetExplainAnalyzeRowsForPlan get explain rows for plan.
func GetExplainAnalyzeRowsForPlan(plan *Explain) (rows [][]string) {
	if err := plan.prepareSchema(); err != nil {
		return rows
	}
	if err := plan.RenderResult(); err != nil {
		return rows
	}
	return plan.Rows
}

// prepareSchema prepares explain's result schema.
func (e *Explain) prepareSchema() error {
	var fieldNames []string
	format := strings.ToLower(e.Format)
	if format == types.ExplainFormatTraditional {
		format = types.ExplainFormatROW
		e.Format = types.ExplainFormatROW
	}
	switch {
	case (format == types.ExplainFormatROW || format == types.ExplainFormatBrief || format == types.ExplainFormatPlanCache) && (!e.Analyze && e.RuntimeStatsColl == nil):
		fieldNames = []string{"id", "estRows", "task", "access object", "operator info"}
	case format == types.ExplainFormatVerbose:
		if e.Analyze || e.RuntimeStatsColl != nil {
			fieldNames = []string{"id", "estRows", "estCost", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
		} else {
			fieldNames = []string{"id", "estRows", "estCost", "task", "access object", "operator info"}
		}
	case format == types.ExplainFormatTrueCardCost:
		fieldNames = []string{"id", "estRows", "estCost", "costFormula", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
	case format == types.ExplainFormatCostTrace:
		if e.Analyze || e.RuntimeStatsColl != nil {
			fieldNames = []string{"id", "estRows", "estCost", "costFormula", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
		} else {
			fieldNames = []string{"id", "estRows", "estCost", "costFormula", "task", "access object", "operator info"}
		}
	case (format == types.ExplainFormatROW || format == types.ExplainFormatBrief || format == types.ExplainFormatPlanCache) && (e.Analyze || e.RuntimeStatsColl != nil):
		fieldNames = []string{"id", "estRows", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
	case format == types.ExplainFormatDOT:
		fieldNames = []string{"dot contents"}
	case format == types.ExplainFormatHint:
		fieldNames = []string{"hint"}
	case format == types.ExplainFormatBinary:
		fieldNames = []string{"binary plan"}
	case format == types.ExplainFormatTiDBJSON:
		fieldNames = []string{"TiDB_JSON"}
	case e.Explore:
		fieldNames = []string{"statement", "binding_hint", "plan", "plan_digest", "avg_latency", "exec_times", "avg_scan_rows",
			"avg_returned_rows", "latency_per_returned_row", "scan_rows_per_returned_row", "recommend", "reason",
			"explain_analyze", "binding"}
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}

	cwn := &columnsWithNames{
		cols:  make([]*expression.Column, 0, len(fieldNames)),
		names: make([]*types.FieldName, 0, len(fieldNames)),
	}

	for _, fieldName := range fieldNames {
		cwn.Append(buildColumnWithName("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
	}
	e.SetSchema(cwn.col2Schema())
	e.names = cwn.names
	return nil
}

func (e *Explain) renderResultForExplore() error {
	bindingHandle := domain.GetDomain(e.SCtx()).BindingHandle()
	sqlOrDigest := e.SQLDigest
	if sqlOrDigest == "" {
		sqlOrDigest = e.ExecStmt.Text()
	}
	plans, err := bindingHandle.ExplorePlansForSQL(e.SCtx(), sqlOrDigest, e.Analyze)
	if err != nil {
		return err
	}
	for _, p := range plans {
		hintStr, err := p.Binding.Hint.Restore()
		if err != nil {
			return err
		}

		e.Rows = append(e.Rows, []string{
			p.Binding.OriginalSQL,
			hintStr,
			p.Plan,
			p.PlanDigest,
			strconv.FormatFloat(p.AvgLatency, 'f', -1, 64),
			strconv.Itoa(int(p.ExecTimes)),
			strconv.FormatFloat(p.AvgScanRows, 'f', -1, 64),
			strconv.FormatFloat(p.AvgReturnedRows, 'f', -1, 64),
			strconv.FormatFloat(p.LatencyPerReturnRow, 'f', -1, 64),
			strconv.FormatFloat(p.ScanRowsPerReturnRow, 'f', -1, 64),
			p.Recommend,
			p.Reason,
			fmt.Sprintf("EXPLAIN ANALYZE '%v'", p.PlanDigest),
			fmt.Sprintf("CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST '%v'", p.PlanDigest)})
	}
	return nil
}

// RenderResult renders the explain result as specified format.
func (e *Explain) RenderResult() error {
	if e.Explore {
		return e.renderResultForExplore()
	}

	if e.TargetPlan == nil {
		return nil
	}

	if e.Analyze && e.Format == types.ExplainFormatTrueCardCost {
		// true_card_cost mode is used to calibrate the cost model.
		pp, ok := e.TargetPlan.(base.PhysicalPlan)
		if ok {
			if _, err := getPlanCost(pp, property.RootTaskType,
				optimizetrace.NewDefaultPlanCostOption().WithCostFlag(costusage.CostFlagRecalculate|costusage.CostFlagUseTrueCardinality|costusage.CostFlagTrace)); err != nil {
				return err
			}
			if pp.SCtx().GetSessionVars().CostModelVersion == modelVer2 {
				// output cost formula and factor costs through warning under model ver2 and true_card_cost mode for cost calibration.
				cost, _ := pp.GetPlanCostVer2(property.RootTaskType, optimizetrace.NewDefaultPlanCostOption())
				if cost.GetTrace() != nil {
					trace := cost.GetTrace()
					pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("cost formula: %v", trace.GetFormula()))
					data, err := json.Marshal(trace.GetFactorCosts())
					if err != nil {
						pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("marshal factor costs error %v", err))
					}
					pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("factor costs: %v", string(data)))

					// output cost factor weights for cost calibration
					factors := defaultVer2Factors.tolist()
					weights := make(map[string]float64)
					for _, factor := range factors {
						if factorCost, ok := trace.GetFactorCosts()[factor.Name]; ok && factor.Value > 0 {
							weights[factor.Name] = factorCost / factor.Value // cost = [factors] * [weights]
						}
					}
					if wstr, err := json.Marshal(weights); err != nil {
						pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("marshal weights error %v", err))
					} else {
						pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("factor weights: %v", string(wstr)))
					}
				}
			}
		} else {
			e.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("'explain format=true_card_cost' cannot support this plan"))
		}
	}
	// For explain for connection, we can directly decode the binary plan to get the explain rows.
	if e.BriefBinaryPlan != "" {
		if strings.ToLower(e.Format) != types.ExplainFormatBrief && strings.ToLower(e.Format) != types.ExplainFormatROW && strings.ToLower(e.Format) != types.ExplainFormatVerbose {
			return errors.Errorf("explain format '%s' for connection is not supported now", e.Format)
		}
		rows, err := plancodec.DecodeBinaryPlan4Connection(e.BriefBinaryPlan, strings.ToLower(e.Format), false)
		if err != nil {
			return err
		}
		e.Rows = rows
		return nil
	}
	switch strings.ToLower(e.Format) {
	case types.ExplainFormatROW, types.ExplainFormatBrief, types.ExplainFormatVerbose, types.ExplainFormatTrueCardCost, types.ExplainFormatCostTrace, types.ExplainFormatPlanCache:
		if e.Rows == nil || e.Analyze {
			flat := FlattenPhysicalPlan(e.TargetPlan, true)
			e.Rows = ExplainFlatPlanInRowFormat(flat, e.Format, e.Analyze, e.RuntimeStatsColl)
			if e.Analyze &&
				e.SCtx().GetSessionVars().MemoryDebugModeMinHeapInUse != 0 &&
				e.SCtx().GetSessionVars().MemoryDebugModeAlarmRatio > 0 {
				row := e.Rows[0]
				tracker := e.SCtx().GetSessionVars().MemTracker
				row[7] = row[7] + "(Total: " + tracker.FormatBytes(tracker.MaxConsumed()) + ")"
			}
		}
	case types.ExplainFormatDOT:
		if physicalPlan, ok := e.TargetPlan.(base.PhysicalPlan); ok {
			e.prepareDotInfo(physicalPlan)
		}
	case types.ExplainFormatHint:
		flat := FlattenPhysicalPlan(e.TargetPlan, false)
		hints := GenHintsFromFlatPlan(flat)
		hints = append(hints, hint.ExtractTableHintsFromStmtNode(e.ExecStmt, nil)...)
		e.Rows = append(e.Rows, []string{hint.RestoreOptimizerHints(hints)})
	case types.ExplainFormatBinary:
		flat := FlattenPhysicalPlan(e.TargetPlan, false)
		str := BinaryPlanStrFromFlatPlan(e.SCtx(), flat, false)
		e.Rows = append(e.Rows, []string{str})
	case types.ExplainFormatTiDBJSON:
		flat := FlattenPhysicalPlan(e.TargetPlan, true)
		encodes := e.explainFlatPlanInJSONFormat(flat)
		if e.Analyze && len(encodes) > 0 &&
			e.SCtx().GetSessionVars().MemoryDebugModeMinHeapInUse != 0 &&
			e.SCtx().GetSessionVars().MemoryDebugModeAlarmRatio > 0 {
			encodeRoot := encodes[0]
			tracker := e.SCtx().GetSessionVars().MemTracker
			encodeRoot.TotalMemoryConsumed = tracker.FormatBytes(tracker.MaxConsumed())
		}
		str, err := JSONToString(encodes)
		if err != nil {
			return err
		}
		e.Rows = append(e.Rows, []string{str})
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}
	return nil
}

// ExplainFlatPlanInRowFormat returns the explain result in row format.
func ExplainFlatPlanInRowFormat(flat *FlatPhysicalPlan, format string, analyze bool,
	runtimeStatsColl *execdetails.RuntimeStatsColl) (rows [][]string) {
	if flat == nil || len(flat.Main) == 0 || flat.InExplain {
		return
	}
	for _, flatOp := range flat.Main {
		rows = prepareOperatorInfo(flatOp, format, analyze,
			runtimeStatsColl, rows)
	}
	for _, cte := range flat.CTEs {
		for _, flatOp := range cte {
			rows = prepareOperatorInfo(flatOp, format, analyze,
				runtimeStatsColl, rows)
		}
	}
	for _, subQ := range flat.ScalarSubQueries {
		for _, flatOp := range subQ {
			rows = prepareOperatorInfo(flatOp, format, analyze,
				runtimeStatsColl, rows)
		}
	}
	return
}

func (e *Explain) explainFlatPlanInJSONFormat(flat *FlatPhysicalPlan) (encodes []*ExplainInfoForEncode) {
	if flat == nil || len(flat.Main) == 0 || flat.InExplain {
		return
	}
	// flat.Main[0] must be the root node of tree
	encodes = append(encodes, e.explainOpRecursivelyInJSONFormat(flat.Main[0], flat.Main))

	for _, cte := range flat.CTEs {
		encodes = append(encodes, e.explainOpRecursivelyInJSONFormat(cte[0], cte))
	}
	for _, subQ := range flat.ScalarSubQueries {
		encodes = append(encodes, e.explainOpRecursivelyInJSONFormat(subQ[0], subQ))
	}
	return
}

func (e *Explain) explainOpRecursivelyInJSONFormat(flatOp *FlatOperator, flats FlatPlanTree) *ExplainInfoForEncode {
	taskTp := ""
	if flatOp.IsRoot {
		taskTp = "root"
	} else {
		taskTp = flatOp.ReqType.Name() + "[" + flatOp.StoreType.Name() + "]"
	}
	explainID := flatOp.ExplainID().String() + flatOp.Label.String()

	cur := e.prepareOperatorInfoForJSONFormat(flatOp.Origin, taskTp, explainID)

	for _, idx := range flatOp.ChildrenIdx {
		cur.SubOperators = append(cur.SubOperators,
			e.explainOpRecursivelyInJSONFormat(flats[idx], flats))
	}
	return cur
}

func getExplainIDAndTaskTp(flatOp *FlatOperator) (taskTp, textTreeExplainID string) {
	if flatOp.IsRoot {
		taskTp = "root"
	} else {
		taskTp = flatOp.ReqType.Name() + "[" + flatOp.StoreType.Name() + "]"
	}
	textTreeExplainID = texttree.PrettyIdentifier(flatOp.ExplainID().String()+flatOp.Label.String(),
		flatOp.TextTreeIndent,
		flatOp.IsLastChild)
	return
}

func getRuntimeInfoStr(ctx base.PlanContext, p base.Plan, runtimeStatsColl *execdetails.RuntimeStatsColl) (actRows, analyzeInfo, memoryInfo, diskInfo string) {
	if runtimeStatsColl == nil {
		runtimeStatsColl = ctx.GetSessionVars().StmtCtx.RuntimeStatsColl
		if runtimeStatsColl == nil {
			return
		}
	}
	rootStats, copStats, memTracker, diskTracker := getRuntimeInfo(ctx, p, runtimeStatsColl)
	actRows = "0"
	memoryInfo = "N/A"
	diskInfo = "N/A"
	if rootStats != nil {
		actRows = strconv.FormatInt(rootStats.GetActRows(), 10)
		analyzeInfo = rootStats.String()
	}
	if copStats != nil {
		if len(analyzeInfo) > 0 {
			analyzeInfo += ", "
		}
		analyzeInfo += copStats.String()
		actRows = strconv.FormatInt(copStats.GetActRows(), 10)
	}
	if memTracker != nil {
		memoryInfo = memTracker.FormatBytes(memTracker.MaxConsumed())
	}
	if diskTracker != nil {
		diskInfo = diskTracker.FormatBytes(diskTracker.MaxConsumed())
	}
	return
}

func getRuntimeInfo(ctx base.PlanContext, p base.Plan, runtimeStatsColl *execdetails.RuntimeStatsColl) (
	rootStats *execdetails.RootRuntimeStats,
	copStats *execdetails.CopRuntimeStats,
	memTracker *memory.Tracker,
	diskTracker *memory.Tracker,
) {
	if runtimeStatsColl == nil {
		runtimeStatsColl = ctx.GetSessionVars().StmtCtx.RuntimeStatsColl
	}
	explainID := p.ID()
	// There maybe some mock information for cop task to let runtimeStatsColl.Exists(p.ExplainID()) is true.
	// So check copTaskExecDetail first and print the real cop task information if it's not empty.
	if runtimeStatsColl != nil && runtimeStatsColl.ExistsRootStats(explainID) {
		rootStats = runtimeStatsColl.GetRootStats(explainID)
	}
	if runtimeStatsColl != nil && runtimeStatsColl.ExistsCopStats(explainID) {
		copStats = runtimeStatsColl.GetCopStats(explainID)
	}
	memTracker = ctx.GetSessionVars().StmtCtx.MemTracker.SearchTrackerWithoutLock(p.ID())
	diskTracker = ctx.GetSessionVars().StmtCtx.DiskTracker.SearchTrackerWithoutLock(p.ID())
	return
}

// prepareOperatorInfo generates the following information for every plan:
// operator id, estimated rows, task type, access object and other operator info.
func prepareOperatorInfo(flatOp *FlatOperator, format string, analyze bool,
	runtimeStatsColl *execdetails.RuntimeStatsColl, rows [][]string) [][]string {
	p := flatOp.Origin
	if p.ExplainID().String() == "_0" {
		return rows
	}
	taskType, id := getExplainIDAndTaskTp(flatOp)

	estRows, estCost, costFormula, accessObject, operatorInfo := getOperatorInfo(p)

	var row []string
	if analyze || runtimeStatsColl != nil {
		row = []string{id, estRows}
		if format == types.ExplainFormatVerbose || format == types.ExplainFormatTrueCardCost || format == types.ExplainFormatCostTrace {
			row = append(row, estCost)
		}
		if format == types.ExplainFormatTrueCardCost || format == types.ExplainFormatCostTrace {
			row = append(row, costFormula)
		}
		actRows, analyzeInfo, memoryInfo, diskInfo := getRuntimeInfoStr(p.SCtx(), p, runtimeStatsColl)
		row = append(row, actRows, taskType, accessObject, analyzeInfo, operatorInfo, memoryInfo, diskInfo)
	} else {
		row = []string{id, estRows}
		if format == types.ExplainFormatVerbose || format == types.ExplainFormatTrueCardCost ||
			format == types.ExplainFormatCostTrace {
			row = append(row, estCost)
		}
		if format == types.ExplainFormatCostTrace {
			row = append(row, costFormula)
		}
		row = append(row, taskType, accessObject, operatorInfo)
	}
	return append(rows, row)
}

func (e *Explain) prepareOperatorInfoForJSONFormat(p base.Plan, taskType, explainID string) *ExplainInfoForEncode {
	if p.ExplainID().String() == "_0" {
		return nil
	}

	estRows, _, _, accessObject, operatorInfo := getOperatorInfo(p)
	jsonRow := &ExplainInfoForEncode{
		ID:           explainID,
		EstRows:      estRows,
		TaskType:     taskType,
		AccessObject: accessObject,
		OperatorInfo: operatorInfo,
		SubOperators: make([]*ExplainInfoForEncode, 0),
	}

	if e.Analyze || e.RuntimeStatsColl != nil {
		jsonRow.ActRows, jsonRow.ExecuteInfo, jsonRow.MemoryInfo, jsonRow.DiskInfo = getRuntimeInfoStr(e.SCtx(), p, e.RuntimeStatsColl)
	}
	return jsonRow
}

func getOperatorInfo(p base.Plan) (estRows, estCost, costFormula, accessObject, operatorInfo string) {
	pp, isPhysicalPlan := p.(base.PhysicalPlan)
	estRows = "N/A"
	estCost = "N/A"
	costFormula = "N/A"
	sctx := p.SCtx()
	if isPhysicalPlan {
		estRows = strconv.FormatFloat(pp.GetEstRowCountForDisplay(), 'f', 2, 64)
		if sctx != nil && sctx.GetSessionVars().CostModelVersion == modelVer2 {
			costVer2, _ := pp.GetPlanCostVer2(property.RootTaskType, optimizetrace.NewDefaultPlanCostOption())
			estCost = strconv.FormatFloat(costVer2.GetCost(), 'f', 2, 64)
			if costVer2.GetTrace() != nil {
				costFormula = costVer2.GetTrace().GetFormula()
			}
		} else {
			planCost, _ := getPlanCost(pp, property.RootTaskType, optimizetrace.NewDefaultPlanCostOption())
			estCost = strconv.FormatFloat(planCost, 'f', 2, 64)
		}
	} else if si := p.StatsInfo(); si != nil {
		estRows = strconv.FormatFloat(si.RowCount, 'f', 2, 64)
	}

	if plan, ok := p.(base.DataAccesser); ok {
		accessObject = plan.AccessObject().String()
		operatorInfo = plan.OperatorInfo(false)
	} else {
		if pa, ok := p.(base.PartitionAccesser); ok && sctx != nil {
			accessObject = pa.AccessObject(sctx).String()
		}
		operatorInfo = p.ExplainInfo()
	}
	return estRows, estCost, costFormula, accessObject, operatorInfo
}

// BinaryPlanStrFromFlatPlan generates the compressed and encoded binary plan from a FlatPhysicalPlan.
func BinaryPlanStrFromFlatPlan(explainCtx base.PlanContext, flat *FlatPhysicalPlan, briefBinaryPlan bool) string {
	binary := binaryDataFromFlatPlan(explainCtx, flat, briefBinaryPlan)
	if binary == nil {
		return ""
	}
	proto, err := binary.Marshal()
	if err != nil {
		return ""
	}
	str := plancodec.Compress(proto)
	return str
}

func binaryDataFromFlatPlan(explainCtx base.PlanContext, flat *FlatPhysicalPlan, briefBinaryPlan bool) *tipb.ExplainData {
	if len(flat.Main) == 0 {
		return nil
	}
	// Please see comments in EncodeFlatPlan() for this case.
	// We keep consistency with EncodeFlatPlan() here.
	if flat.InExecute {
		return nil
	}
	res := &tipb.ExplainData{}
	for _, op := range flat.Main {
		// We assume that runtime stats are available to this plan tree if any operator in the "Main" has runtime stats.
		rootStats, copStats, _, _ := getRuntimeInfo(explainCtx, op.Origin, nil)
		if rootStats != nil || copStats != nil {
			res.WithRuntimeStats = true
			break
		}
	}
	res.Main = binaryOpTreeFromFlatOps(explainCtx, flat.Main, briefBinaryPlan)
	for _, explainedCTE := range flat.CTEs {
		res.Ctes = append(res.Ctes, binaryOpTreeFromFlatOps(explainCtx, explainedCTE, briefBinaryPlan))
	}
	return res
}

func binaryOpTreeFromFlatOps(explainCtx base.PlanContext, ops FlatPlanTree, briefBinaryPlan bool) *tipb.ExplainOperator {
	operators := make([]tipb.ExplainOperator, len(ops))

	// First phase: Generate all operators with normal processing (including ID suffix)
	for i, op := range ops {
		binaryOpFromFlatOp(explainCtx, op, &operators[i])
	}

	// Second phase: If briefBinaryPlan is true, set IgnoreExplainIDSuffix and generate BriefName
	if briefBinaryPlan && len(ops) > 0 && ops[0].Origin.SCtx() != nil {
		stmtCtx := ops[0].Origin.SCtx().GetSessionVars().StmtCtx
		originalIgnore := stmtCtx.IgnoreExplainIDSuffix
		stmtCtx.IgnoreExplainIDSuffix = true

		for i, op := range ops {
			operators[i].BriefName = op.ExplainID().String()
			switch op.Origin.(type) {
			case *PhysicalTableReader, *PhysicalIndexReader, *PhysicalHashJoin, *physicalop.PhysicalIndexJoin, *PhysicalIndexHashJoin, *PhysicalMergeJoin:
				operators[i].BriefOperatorInfo = op.Origin.ExplainInfo()
			}
		}

		stmtCtx.IgnoreExplainIDSuffix = originalIgnore
	}

	// Build the tree structure
	for i, op := range ops {
		for _, idx := range op.ChildrenIdx {
			operators[i].Children = append(operators[i].Children, &operators[idx])
		}
	}
	return &operators[0]
}

func binaryOpFromFlatOp(explainCtx base.PlanContext, fop *FlatOperator, out *tipb.ExplainOperator) {
	out.Name = fop.ExplainID().String()
	switch fop.Label {
	case BuildSide:
		out.Labels = []tipb.OperatorLabel{tipb.OperatorLabel_buildSide}
	case ProbeSide:
		out.Labels = []tipb.OperatorLabel{tipb.OperatorLabel_probeSide}
	case SeedPart:
		out.Labels = []tipb.OperatorLabel{tipb.OperatorLabel_seedPart}
	case RecursivePart:
		out.Labels = []tipb.OperatorLabel{tipb.OperatorLabel_recursivePart}
	}
	switch fop.StoreType {
	case kv.TiDB:
		out.StoreType = tipb.StoreType_tidb
	case kv.TiKV:
		out.StoreType = tipb.StoreType_tikv
	case kv.TiFlash:
		out.StoreType = tipb.StoreType_tiflash
	}
	if fop.IsRoot {
		out.TaskType = tipb.TaskType_root
	} else {
		switch fop.ReqType {
		case Cop:
			out.TaskType = tipb.TaskType_cop
		case BatchCop:
			out.TaskType = tipb.TaskType_batchCop
		case MPP:
			out.TaskType = tipb.TaskType_mpp
		}
	}

	if fop.IsPhysicalPlan {
		p := fop.Origin.(base.PhysicalPlan)
		out.Cost, _ = getPlanCost(p, property.RootTaskType, optimizetrace.NewDefaultPlanCostOption())
		out.EstRows = p.GetEstRowCountForDisplay()
	} else if statsInfo := fop.Origin.StatsInfo(); statsInfo != nil {
		out.EstRows = statsInfo.RowCount
	}

	// Runtime info
	rootStats, copStats, memTracker, diskTracker := getRuntimeInfo(explainCtx, fop.Origin, nil)
	if rootStats != nil {
		basic, groups := rootStats.MergeStats()
		if basic != nil {
			out.RootBasicExecInfo = basic.String()
		}
		for _, group := range groups {
			str := group.String()
			if len(str) > 0 {
				out.RootGroupExecInfo = append(out.RootGroupExecInfo, str)
			}
		}
		out.ActRows = uint64(rootStats.GetActRows())
	}
	if copStats != nil {
		out.CopExecInfo = copStats.String()
		out.ActRows = uint64(copStats.GetActRows())
	}
	if memTracker != nil {
		out.MemoryBytes = memTracker.MaxConsumed()
	} else {
		out.MemoryBytes = -1
	}
	if diskTracker != nil {
		out.DiskBytes = diskTracker.MaxConsumed()
	} else {
		out.DiskBytes = -1
	}

	// Operator info
	if plan, ok := fop.Origin.(base.DataAccesser); ok {
		out.OperatorInfo = plan.OperatorInfo(false)
	} else {
		out.OperatorInfo = fop.Origin.ExplainInfo()
	}
	// Access object
	switch p := fop.Origin.(type) {
	case base.DataAccesser:
		ao := p.AccessObject()
		if ao != nil {
			ao.SetIntoPB(out)
		}
	case base.PartitionAccesser:
		ao := p.AccessObject(explainCtx)
		if ao != nil {
			ao.SetIntoPB(out)
		}
	}
}

func (e *Explain) prepareDotInfo(p base.PhysicalPlan) {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "\ndigraph %s {\n", p.ExplainID())
	e.prepareTaskDot(&pair{p, false}, "root", buffer)
	buffer.WriteString("}\n")

	e.Rows = append(e.Rows, []string{buffer.String()})
}

type pair struct {
	physicalPlan base.PhysicalPlan
	isChildOfINL bool
}

func (e *Explain) prepareTaskDot(pa *pair, taskTp string, buffer *bytes.Buffer) {
	fmt.Fprintf(buffer, "subgraph cluster%v{\n", pa.physicalPlan.ID())
	buffer.WriteString("node [style=filled, color=lightgrey]\n")
	buffer.WriteString("color=black\n")
	fmt.Fprintf(buffer, "label = \"%s\"\n", taskTp)

	if len(pa.physicalPlan.Children()) == 0 {
		if taskTp == "cop" {
			fmt.Fprintf(buffer, "\"%s\"\n}\n", pa.physicalPlan.ExplainID(pa.isChildOfINL))
			return
		}
		fmt.Fprintf(buffer, "\"%s\"\n", pa.physicalPlan.ExplainID(pa.isChildOfINL))
	}
	var copTasks []*pair
	var pipelines []string

	for planQueue := []*pair{pa}; len(planQueue) > 0; planQueue = planQueue[1:] {
		curPair := planQueue[0]
		switch copPlan := curPair.physicalPlan.(type) {
		case *PhysicalTableReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			copTasks = append(copTasks, &pair{physicalPlan: copPlan.tablePlan})
		case *PhysicalIndexReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, &pair{physicalPlan: copPlan.indexPlan})
		case *PhysicalIndexLookUpReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, &pair{physicalPlan: copPlan.tablePlan, isChildOfINL: true})
			copTasks = append(copTasks, &pair{physicalPlan: copPlan.indexPlan})
		case *PhysicalIndexMergeReader:
			for i := range copPlan.partialPlans {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.partialPlans[i].ExplainID()))
				copTasks = append(copTasks, &pair{physicalPlan: copPlan.partialPlans[i]})
			}
			if copPlan.tablePlan != nil {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
				copTasks = append(copTasks, &pair{physicalPlan: copPlan.tablePlan, isChildOfINL: true})
			}
		}
		for _, child := range curPair.physicalPlan.Children() {
			fmt.Fprintf(buffer, "\"%s\" -> \"%s\"\n", curPair.physicalPlan.ExplainID(curPair.isChildOfINL), child.ExplainID(curPair.isChildOfINL))
			// pass current pair isChildOfINL.
			planQueue = append(planQueue, &pair{physicalPlan: child, isChildOfINL: curPair.isChildOfINL})
		}
	}
	buffer.WriteString("}\n")

	for _, cop := range copTasks {
		e.prepareTaskDot(cop, "cop", buffer)
	}

	for i := range pipelines {
		buffer.WriteString(pipelines[i])
	}
}

// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. session is not InTxn
//  3. plan is point get by pk, or point get by unique index (no double read)
func IsPointGetWithPKOrUniqueKeyByAutoCommit(vars *variable.SessionVars, p base.Plan) bool {
	if !IsAutoCommitTxn(vars) {
		return false
	}

	// check plan
	if proj, ok := p.(*physicalop.PhysicalProjection); ok {
		p = proj.Children()[0]
	}

	switch v := p.(type) {
	case *PhysicalIndexReader:
		indexScan := v.IndexPlans[0].(*PhysicalIndexScan)
		return indexScan.IsPointGetByUniqueKey(vars.StmtCtx.TypeCtx())
	case *PhysicalTableReader:
		tableScan, ok := v.TablePlans[0].(*PhysicalTableScan)
		if !ok {
			return false
		}
		isPointRange := len(tableScan.Ranges) == 1 && tableScan.Ranges[0].IsPointNonNullable(vars.StmtCtx.TypeCtx())
		if !isPointRange {
			return false
		}
		pkLength := 1
		if tableScan.Table.IsCommonHandle {
			pkIdx := tables.FindPrimaryIndex(tableScan.Table)
			pkLength = len(pkIdx.Columns)
		}
		return len(tableScan.Ranges[0].LowVal) == pkLength
	case *PointGetPlan:
		// If the PointGetPlan needs to read data using unique index (double read), we
		// can't use max uint64, because using math.MaxUint64 can't guarantee repeatable-read
		// and the data and index would be inconsistent!
		// If the PointGetPlan needs to read data from Cache Table, we can't use max uint64,
		// because math.MaxUint64 always make cacheData invalid.
		noSecondRead := v.IndexInfo == nil || (v.IndexInfo.Primary && v.TblInfo.IsCommonHandle)
		if !noSecondRead {
			return false
		}
		if v.TblInfo != nil && (v.TblInfo.TableCacheStatusType != model.TableCacheStatusDisable) {
			return false
		}
		return true
	default:
		return false
	}
}

// IsAutoCommitTxn checks if session is in autocommit mode and not InTxn
// used for fast plan like point get
func IsAutoCommitTxn(vars *variable.SessionVars) bool {
	return vars.IsAutocommit() && !vars.InTxn()
}

// AdminShowBDRRole represents a show bdr role plan.
type AdminShowBDRRole struct {
	baseSchemaProducer
}
