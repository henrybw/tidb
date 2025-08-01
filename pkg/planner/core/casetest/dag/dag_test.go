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

package dag

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func assertSameHints(t *testing.T, expected, actual []*ast.TableOptimizerHint) {
	expectedStr := make([]string, 0, len(expected))
	actualStr := make([]string, 0, len(actual))
	for _, h := range expected {
		expectedStr = append(expectedStr, hint.RestoreTableOptimizerHint(h))
	}
	for _, h := range actual {
		actualStr = append(actualStr, hint.RestoreTableOptimizerHint(h))
	}
	require.ElementsMatch(t, expectedStr, actualStr)
}

func TestDAGPlanBuilderSimpleCase(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("set tidb_opt_limit_push_down_threshold=0")
		var input []string
		var output []struct {
			SQL  string
			Best string
		}
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		p := parser.New()
		is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
		for i, tt := range input {
			comment := fmt.Sprintf("case: %v, sql: %s", i, tt)
			stmt, err := p.ParseOneStmt(tt, "", "")
			require.NoError(t, err, comment)
			require.NoError(t, sessiontxn.NewTxn(context.Background(), testKit.Session()))
			testKit.Session().GetSessionVars().StmtCtx.OriginalSQL = tt
			nodeW := resolve.NewNodeW(stmt)
			p, _, err := planner.Optimize(context.TODO(), testKit.Session(), nodeW, is)
			require.NoError(t, err)
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Best = core.ToString(p)
			})
			require.Equal(t, output[i].Best, core.ToString(p), comment)
		}
	})
}

func TestDAGPlanBuilderJoin(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		sessionVars := testKit.Session().GetSessionVars()
		sessionVars.ExecutorConcurrency = 4
		sessionVars.SetDistSQLScanConcurrency(15)
		sessionVars.SetHashJoinConcurrency(5)

		var input []string
		var output []struct {
			SQL  string
			Best string
		}
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output, cascades, caller)

		p := parser.New()
		is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
		for i, tt := range input {
			comment := fmt.Sprintf("case:%v sql:%s", i, tt)
			stmt, err := p.ParseOneStmt(tt, "", "")
			require.NoError(t, err, comment)

			nodeW := resolve.NewNodeW(stmt)
			p, _, err := planner.Optimize(context.TODO(), testKit.Session(), nodeW, is)
			require.NoError(t, err)
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Best = core.ToString(p)
			})
			require.Equal(t, output[i].Best, core.ToString(p), comment)
		}
	})
}

func TestDAGPlanBuilderSubquery(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")

		testKit.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
		sessionVars := testKit.Session().GetSessionVars()
		sessionVars.SetHashAggFinalConcurrency(1)
		sessionVars.SetHashAggPartialConcurrency(1)
		sessionVars.SetHashJoinConcurrency(5)
		sessionVars.SetDistSQLScanConcurrency(15)
		sessionVars.ExecutorConcurrency = 4
		var input []string
		var output []struct {
			SQL  string
			Best string
		}
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		p := parser.New()
		is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
		for i, tt := range input {
			comment := fmt.Sprintf("input: %s", tt)
			stmt, err := p.ParseOneStmt(tt, "", "")
			require.NoError(t, err, comment)

			nodeW := resolve.NewNodeW(stmt)
			p, _, err := planner.Optimize(context.TODO(), testKit.Session(), nodeW, is)
			require.NoError(t, err)
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Best = core.ToString(p)
			})
			require.Equal(t, output[i].Best, core.ToString(p), fmt.Sprintf("input: %s", tt))
		}
	})
}

func TestDAGPlanTopN(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")

		var input []string
		var output []struct {
			SQL  string
			Best string
		}
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		p := parser.New()
		is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
		for i, tt := range input {
			comment := fmt.Sprintf("case:%v sql:%s", i, tt)
			stmt, err := p.ParseOneStmt(tt, "", "")
			require.NoError(t, err, comment)

			nodeW := resolve.NewNodeW(stmt)
			p, _, err := planner.Optimize(context.TODO(), testKit.Session(), nodeW, is)
			require.NoError(t, err)
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Best = core.ToString(p)
			})
			require.Equal(t, output[i].Best, core.ToString(p), comment)
		}
	})
}

func TestDAGPlanBuilderBasePhysicalPlan(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		se, err := session.CreateSession4Test(testKit.Session().GetStore())
		require.NoError(t, err)
		_, err = se.Execute(context.Background(), "use test")
		require.NoError(t, err)

		var input []string
		var output []struct {
			SQL   string
			Best  string
			Hints string
		}
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		p := parser.New()
		is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
		for i, tt := range input {
			comment := fmt.Sprintf("input: %s", tt)
			stmt, err := p.ParseOneStmt(tt, "", "")
			require.NoError(t, err, comment)
			nodeW := resolve.NewNodeW(stmt)
			err = core.Preprocess(context.Background(), se, nodeW, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is}))
			require.NoError(t, err)
			p, _, err := planner.Optimize(context.TODO(), se, nodeW, is)
			require.NoError(t, err)
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Best = core.ToString(p)
				output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
			})
			require.Equal(t, output[i].Best, core.ToString(p), fmt.Sprintf("input: %s", tt))
			hints := core.GenHintsFromPhysicalPlan(p)

			// test the new genHints code
			flat := core.FlattenPhysicalPlan(p, false)
			newHints := core.GenHintsFromFlatPlan(flat)
			assertSameHints(t, hints, newHints)

			require.Equal(t, output[i].Hints, hint.RestoreOptimizerHints(hints), fmt.Sprintf("input: %s", tt))
		}
	})
}

func TestDAGPlanBuilderUnion(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")

		var input []string
		var output []struct {
			SQL  string
			Best string
		}
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		p := parser.New()
		is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
		for i, tt := range input {
			comment := fmt.Sprintf("case:%v sql:%s", i, tt)
			stmt, err := p.ParseOneStmt(tt, "", "")
			require.NoError(t, err, comment)

			nodeW := resolve.NewNodeW(stmt)
			p, _, err := planner.Optimize(context.TODO(), testKit.Session(), nodeW, is)
			require.NoError(t, err)
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Best = core.ToString(p)
			})
			require.Equal(t, output[i].Best, core.ToString(p), comment)
		}
	})
}

func TestDAGPlanBuilderUnionScan(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int, b int, c int)")

		var input []string
		var output []struct {
			SQL  string
			Best string
		}
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output, cascades, caller)

		p := parser.New()
		for i, tt := range input {
			testKit.MustExec("begin;")
			testKit.MustExec("insert into t values(2, 2, 2);")

			comment := fmt.Sprintf("input: %s", tt)
			stmt, err := p.ParseOneStmt(tt, "", "")
			require.NoError(t, err, comment)
			dom := domain.GetDomain(testKit.Session())
			require.NoError(t, dom.Reload())
			nodeW := resolve.NewNodeW(stmt)
			plan, _, err := planner.Optimize(context.TODO(), testKit.Session(), nodeW, dom.InfoSchema())
			require.NoError(t, err)
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Best = core.ToString(plan)
			})
			require.Equal(t, output[i].Best, core.ToString(plan), fmt.Sprintf("input: %s", tt))
			testKit.MustExec("rollback;")
		}
	})
}

func TestDAGPlanBuilderAgg(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")

		testKit.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
		sessionVars := testKit.Session().GetSessionVars()
		sessionVars.SetHashAggFinalConcurrency(1)
		sessionVars.SetHashAggPartialConcurrency(1)
		sessionVars.SetDistSQLScanConcurrency(15)
		sessionVars.ExecutorConcurrency = 4

		var input []string
		var output []struct {
			SQL  string
			Best string
		}
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		p := parser.New()
		is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
		for i, tt := range input {
			comment := fmt.Sprintf("input: %s", tt)
			stmt, err := p.ParseOneStmt(tt, "", "")
			require.NoError(t, err, comment)

			nodeW := resolve.NewNodeW(stmt)
			p, _, err := planner.Optimize(context.TODO(), testKit.Session(), nodeW, is)
			require.NoError(t, err)
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Best = core.ToString(p)
			})
			require.Equal(t, output[i].Best, core.ToString(p), fmt.Sprintf("input: %s", tt))
		}
	})
}

func doTestDAGPlanBuilderWindow(t *testing.T, vars, input []string, output []struct {
	SQL  string
	Best string
}) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")

		for _, v := range vars {
			testKit.MustExec(v)
		}

		p := parser.New()
		is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

		for i, tt := range input {
			comment := fmt.Sprintf("case:%v sql:%s", i, tt)
			stmt, err := p.ParseOneStmt(tt, "", "")
			require.NoError(t, err, comment)

			err = sessiontxn.NewTxn(context.Background(), testKit.Session())
			require.NoError(t, err)
			nodeW := resolve.NewNodeW(stmt)
			p, _, err := planner.Optimize(context.TODO(), testKit.Session(), nodeW, is)
			require.NoError(t, err)
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Best = core.ToString(p)
			})
			require.Equal(t, output[i].Best, core.ToString(p), comment)
		}
	})
}

func TestDAGPlanBuilderWindow(t *testing.T) {
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	vars := []string{
		"set @@session.tidb_window_concurrency = 1",
	}
	doTestDAGPlanBuilderWindow(t, vars, input, output)
}

func TestDAGPlanBuilderWindowParallel(t *testing.T) {
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	vars := []string{
		"set @@session.tidb_window_concurrency = 4",
	}
	doTestDAGPlanBuilderWindow(t, vars, input, output)
}
