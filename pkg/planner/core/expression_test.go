// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func parseExpr(t *testing.T, expr string) ast.ExprNode {
	p := parser.New()
	st, err := p.ParseOneStmt("select "+expr, "", "")
	require.NoError(t, err)
	stmt := st.(*ast.SelectStmt)
	return stmt.Fields.Fields[0].Expr
}

func buildExpr(t *testing.T, ctx expression.BuildContext, exprNode any, opts ...expression.BuildOption) (expr expression.Expression, err error) {
	switch x := exprNode.(type) {
	case string:
		node := parseExpr(t, x)
		expr, err = expression.BuildSimpleExpr(ctx, node, opts...)
	case ast.ExprNode:
		expr, err = expression.BuildSimpleExpr(ctx, x, opts...)
	default:
		require.FailNow(t, "invalid input type: %T", x)
	}

	if err != nil {
		require.Nil(t, expr)
	} else {
		require.NotNil(t, expr)
	}
	return
}

func buildExprAndEval(t *testing.T, ctx expression.BuildContext, exprNode any) types.Datum {
	expr, err := buildExpr(t, ctx, exprNode)
	require.NoError(t, err)
	val, err := expr.Eval(ctx.GetEvalCtx(), chunk.Row{})
	require.NoError(t, err)
	return val
}

type testCase struct {
	exprStr   string
	resultStr string
}

func runTests(t *testing.T, tests []testCase) {
	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	for _, tt := range tests {
		expr := parseExpr(t, tt.exprStr)
		val, err := evalAstExpr(ctx, expr)
		require.NoError(t, err)
		valStr := fmt.Sprintf("%v", val.GetValue())
		require.Equalf(t, tt.resultStr, valStr, "for %s", tt.exprStr)

		val = buildExprAndEval(t, ctx, expr)
		valStr = fmt.Sprintf("%v", val.GetValue())
		require.Equalf(t, tt.resultStr, valStr, "for %s", tt.exprStr)
	}
}

func TestBetween(t *testing.T) {
	tests := []testCase{
		{exprStr: "1 between 2 and 3", resultStr: "0"},
		{exprStr: "1 not between 2 and 3", resultStr: "1"},
		{exprStr: "'2001-04-10 12:34:56' between cast('2001-01-01 01:01:01' as datetime) and '01-05-01'", resultStr: "1"},
		{exprStr: "20010410123456 between cast('2001-01-01 01:01:01' as datetime) and 010501", resultStr: "0"},
		{exprStr: "20010410123456 between cast('2001-01-01 01:01:01' as datetime) and 20010501123456", resultStr: "1"},
	}
	runTests(t, tests)
}

func TestCaseWhen(t *testing.T) {
	tests := []testCase{
		{
			exprStr:   "case 1 when 1 then 'str1' when 2 then 'str2' end",
			resultStr: "str1",
		},
		{
			exprStr:   "case 2 when 1 then 'str1' when 2 then 'str2' end",
			resultStr: "str2",
		},
		{
			exprStr:   "case 3 when 1 then 'str1' when 2 then 'str2' end",
			resultStr: "<nil>",
		},
		{
			exprStr:   "case 4 when 1 then 'str1' when 2 then 'str2' else 'str3' end",
			resultStr: "str3",
		},
	}
	runTests(t, tests)

	// When expression value changed, result set back to null.
	valExpr := ast.NewValueExpr(1, "", "")
	whenClause := &ast.WhenClause{Expr: ast.NewValueExpr(1, "", ""), Result: ast.NewValueExpr(1, "", "")}
	caseExpr := &ast.CaseExpr{
		Value:       valExpr,
		WhenClauses: []*ast.WhenClause{whenClause},
	}
	ctx := MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	v, err := evalAstExpr(ctx, caseExpr)
	require.NoError(t, err)
	require.Equal(t, types.NewDatum(int64(1)), v)
	require.Equal(t, types.NewDatum(int64(1)), buildExprAndEval(t, ctx, caseExpr))

	valExpr.SetValue(4)
	v, err = evalAstExpr(ctx, caseExpr)
	require.NoError(t, err)
	require.Equal(t, types.KindNull, v.Kind())
	v = buildExprAndEval(t, ctx, caseExpr)
	require.Equal(t, types.KindNull, v.Kind())
}

func TestCast(t *testing.T) {
	f := types.NewFieldType(mysql.TypeLonglong)

	expr := &ast.FuncCastExpr{
		Expr: ast.NewValueExpr(1, "", ""),
		Tp:   f,
	}

	ctx := MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	ast.SetFlag(expr)
	v, err := evalAstExpr(ctx, expr)
	require.NoError(t, err)
	require.Equal(t, types.NewDatum(int64(1)), v)
	require.Equal(t, types.NewDatum(int64(1)), buildExprAndEval(t, ctx, expr))

	f.AddFlag(mysql.UnsignedFlag)
	v, err = evalAstExpr(ctx, expr)
	require.NoError(t, err)
	require.Equal(t, types.NewDatum(uint64(1)), v)
	require.Equal(t, types.NewDatum(uint64(1)), buildExprAndEval(t, ctx, expr))

	f.SetType(mysql.TypeString)
	f.SetCharset(charset.CharsetBin)
	f.SetFlen(-1)
	f.SetDecimal(-1)
	v, err = evalAstExpr(ctx, expr)
	require.NoError(t, err)
	testutil.DatumEqual(t, types.NewDatum([]byte("1")), v)
	testutil.DatumEqual(t, types.NewDatum([]byte("1")), buildExprAndEval(t, ctx, expr))

	f.SetType(mysql.TypeString)
	f.SetCharset(charset.CharsetUTF8)
	f.SetFlen(-1)
	f.SetDecimal(-1)
	v, err = evalAstExpr(ctx, expr)
	require.NoError(t, err)
	testutil.DatumEqual(t, types.NewDatum([]byte("1")), v)
	testutil.DatumEqual(t, types.NewDatum([]byte("1")), buildExprAndEval(t, ctx, expr))

	expr.Expr = ast.NewValueExpr(nil, "", "")
	v, err = evalAstExpr(ctx, expr)
	require.NoError(t, err)
	require.Equal(t, types.KindNull, v.Kind())
	v = buildExprAndEval(t, ctx, expr)
	require.Equal(t, types.KindNull, v.Kind())
}

func TestPatternIn(t *testing.T) {
	tests := []testCase{
		{
			exprStr:   "1 not in (1, 2, 3)",
			resultStr: "0",
		},
		{
			exprStr:   "1 in (1, 2, 3)",
			resultStr: "1",
		},
		{
			exprStr:   "1 in (2, 3)",
			resultStr: "0",
		},
		{
			exprStr:   "NULL in (2, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "NULL not in (2, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "NULL in (NULL, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "1 in (1, NULL)",
			resultStr: "1",
		},
		{
			exprStr:   "1 in (NULL, 1)",
			resultStr: "1",
		},
		{
			exprStr:   "2 in (1, NULL)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "(-(23)++46/51*+51) in (+23)",
			resultStr: "0",
		},
	}
	runTests(t, tests)
}

func TestIsNull(t *testing.T) {
	tests := []testCase{
		{
			exprStr:   "1 IS NULL",
			resultStr: "0",
		},
		{
			exprStr:   "1 IS NOT NULL",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NULL",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT NULL",
			resultStr: "0",
		},
	}
	runTests(t, tests)
}

func TestCompareRow(t *testing.T) {
	tests := []testCase{
		{
			exprStr:   "row(1,2,3)=row(1,2,3)",
			resultStr: "1",
		},
		{
			exprStr:   "row(1,2,3)=row(1+3,2,3)",
			resultStr: "0",
		},
		{
			exprStr:   "row(1,2,3)<>row(1,2,3)",
			resultStr: "0",
		},
		{
			exprStr:   "row(1,2,3)<>row(1+3,2,3)",
			resultStr: "1",
		},
		{
			exprStr:   "row(1+3,2,3)<>row(1+3,2,3)",
			resultStr: "0",
		},
		{
			exprStr:   "row(1,2,3)<row(1,NULL,3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "row(1,2,3)<row(2,NULL,3)",
			resultStr: "1",
		},
		{
			exprStr:   "row(1,2,3)>=row(0,NULL,3)",
			resultStr: "1",
		},
		{
			exprStr:   "row(1,2,3)<=row(2,NULL,3)",
			resultStr: "1",
		},
	}
	runTests(t, tests)
}

func TestIsTruth(t *testing.T) {
	tests := []testCase{
		{
			exprStr:   "1 IS TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "2 IS TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "0 IS TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "NULL IS TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "1 IS FALSE",
			resultStr: "0",
		},
		{
			exprStr:   "2 IS FALSE",
			resultStr: "0",
		},
		{
			exprStr:   "0 IS FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "1 IS NOT TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "2 IS NOT TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "0 IS NOT TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "1 IS NOT FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "2 IS NOT FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "0 IS NOT FALSE",
			resultStr: "0",
		},
		{
			exprStr:   "NULL IS NOT FALSE",
			resultStr: "1",
		},
	}
	runTests(t, tests)
}

func TestBuildExpression(t *testing.T) {
	tbl := &model.TableInfo{
		Columns: []*model.ColumnInfo{
			{
				Name:          ast.NewCIStr("id"),
				Offset:        0,
				State:         model.StatePublic,
				FieldType:     *types.NewFieldType(mysql.TypeString),
				DefaultIsExpr: true,
				DefaultValue:  "uuid()",
			},
			{
				Name:      ast.NewCIStr("a"),
				Offset:    1,
				State:     model.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
			},
			{
				Name:         ast.NewCIStr("b"),
				Offset:       2,
				State:        model.StatePublic,
				FieldType:    *types.NewFieldType(mysql.TypeLonglong),
				DefaultValue: "123",
			},
		},
	}

	ctx := exprstatic.NewExprContext()
	evalCtx := ctx.GetStaticEvalCtx()
	cols, names, err := expression.ColumnInfos2ColumnsAndNames(ctx, ast.NewCIStr(""), tbl.Name, tbl.Cols(), tbl)
	require.NoError(t, err)
	schema := expression.NewSchema(cols...)

	// normal build
	ctx = ctx.Apply(exprstatic.WithColumnIDAllocator(exprctx.NewSimplePlanColumnIDAllocator(0)))
	expr, err := buildExpr(t, ctx, "(1+a)*(3+b)", expression.WithTableInfo("", tbl))
	require.NoError(t, err)
	ctx = ctx.Apply(exprstatic.WithColumnIDAllocator(exprctx.NewSimplePlanColumnIDAllocator(0)))
	expr2, err := expression.ParseSimpleExpr(ctx, "(1+a)*(3+b)", expression.WithTableInfo("", tbl))
	require.NoError(t, err)
	require.True(t, expr.Equal(evalCtx, expr2))
	val, _, err := expr.EvalInt(evalCtx, chunk.MutRowFromValues("", 1, 2).ToRow())
	require.NoError(t, err)
	require.Equal(t, int64(10), val)
	val, _, err = expr.EvalInt(evalCtx, chunk.MutRowFromValues("", 3, 4).ToRow())
	require.NoError(t, err)
	require.Equal(t, int64(28), val)
	val, _, err = expr2.EvalInt(evalCtx, chunk.MutRowFromValues("", 1, 2).ToRow())
	require.NoError(t, err)
	require.Equal(t, int64(10), val)
	val, _, err = expr2.EvalInt(evalCtx, chunk.MutRowFromValues("", 3, 4).ToRow())
	require.NoError(t, err)
	require.Equal(t, int64(28), val)

	expr, err = buildExpr(t, ctx, "(1+a)*(3+b)", expression.WithInputSchemaAndNames(schema, names, nil))
	require.NoError(t, err)
	val, _, err = expr.EvalInt(evalCtx, chunk.MutRowFromValues("", 1, 2).ToRow())
	require.NoError(t, err)
	require.Equal(t, int64(10), val)

	// build expression without enough columns
	_, err = buildExpr(t, ctx, "1+a")
	require.EqualError(t, err, "[planner:1054]Unknown column 'a' in 'expression'")
	_, err = buildExpr(t, ctx, "(1+a)*(3+b+c)", expression.WithTableInfo("", tbl))
	require.EqualError(t, err, "[planner:1054]Unknown column 'c' in 'expression'")

	// cast to array not supported by default
	_, err = buildExpr(t, ctx, "cast(1 as signed array)")
	require.EqualError(t, err, "[expression:1235]This version of TiDB doesn't yet support 'Use of CAST( .. AS .. ARRAY) outside of functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions'")
	// use WithAllowCastArray to allow casting to array
	expr, err = buildExpr(t, ctx, `cast(json_extract('{"a": [1, 2, 3]}', '$.a') as signed array)`, expression.WithAllowCastArray(true))
	require.NoError(t, err)
	j, _, err := expr.EvalJSON(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.JSONTypeCodeArray, j.TypeCode)
	require.Equal(t, "[1, 2, 3]", j.String())

	// default expr
	expr, err = buildExpr(t, ctx, "default(id)", expression.WithTableInfo("", tbl))
	require.NoError(t, err)
	s, _, err := expr.EvalString(evalCtx, chunk.MutRowFromValues("", 1, 2).ToRow())
	require.NoError(t, err)
	require.Equal(t, 36, len(s), s)

	expr, err = buildExpr(t, ctx, "default(id)", expression.WithInputSchemaAndNames(schema, names, tbl))
	require.NoError(t, err)
	s, _, err = expr.EvalString(evalCtx, chunk.MutRowFromValues("", 1, 2).ToRow())
	require.NoError(t, err)
	require.Equal(t, 36, len(s), s)

	expr, err = buildExpr(t, ctx, "default(b)", expression.WithTableInfo("", tbl))
	require.NoError(t, err)
	d, err := expr.Eval(evalCtx, chunk.MutRowFromValues("", 1, 2).ToRow())
	require.NoError(t, err)
	require.Equal(t, types.NewDatum(int64(123)), d)

	// WithCastExprTo
	expr, err = buildExpr(t, ctx, "1+2+3")
	require.NoError(t, err)
	require.Equal(t, mysql.TypeLonglong, expr.GetType(evalCtx).GetType())
	castTo := types.NewFieldType(mysql.TypeVarchar)
	expr, err = buildExpr(t, ctx, "1+2+3", expression.WithCastExprTo(castTo))
	require.NoError(t, err)
	require.Equal(t, mysql.TypeVarchar, expr.GetType(evalCtx).GetType())
	v, err := expr.Eval(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.KindString, v.Kind())
	require.Equal(t, "6", v.GetString())

	// param marker
	params := variable.NewPlanCacheParamList()
	params.Append(types.NewIntDatum(5))
	evalCtx = evalCtx.Apply(exprstatic.WithParamList(params))
	ctx = ctx.Apply(exprstatic.WithEvalCtx(evalCtx))
	expr, err = buildExpr(t, ctx, "a + ?", expression.WithTableInfo("", tbl))
	require.NoError(t, err)
	require.Equal(t, mysql.TypeLonglong, expr.GetType(evalCtx).GetType())
	v, err = expr.Eval(evalCtx, chunk.MutRowFromValues(1, 2, 3).ToRow())
	require.NoError(t, err)
	require.Equal(t, types.KindInt64, v.Kind())
	require.Equal(t, int64(7), v.GetInt64())

	// user variable write needs required option
	_, err = buildExpr(t, ctx, "@a := 1")
	require.EqualError(t, err, "rewriting user variable requires 'OptPropSessionVars' in evalCtx")

	// reading user var
	vars := variable.NewSessionVars(nil)
	vars.TimeZone = evalCtx.Location()
	vars.StmtCtx.SetTimeZone(vars.Location())
	evalCtx = evalCtx.Apply(exprstatic.WithUserVarsReader(vars.GetSessionVars().UserVars))
	ctx = ctx.Apply(exprstatic.WithEvalCtx(evalCtx))
	vars.SetUserVarVal("a", types.NewStringDatum("abc"))
	getVarExpr, err := buildExpr(t, ctx, "@a")
	require.NoError(t, err)
	v, err = getVarExpr.Eval(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.KindString, v.Kind())
	require.Equal(t, "abc", v.GetString())

	// writing user var
	evalCtx = evalCtx.Apply(exprstatic.WithOptionalProperty(expropt.NewSessionVarsProvider(vars)))
	ctx = ctx.Apply(exprstatic.WithEvalCtx(evalCtx))
	expr, err = buildExpr(t, ctx, "@a := 'def'")
	require.NoError(t, err)
	v, err = expr.Eval(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.KindString, v.Kind())
	require.Equal(t, "def", v.GetString())
	v, err = getVarExpr.Eval(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.KindString, v.Kind())
	require.Equal(t, "def", v.GetString())

	// should report error for default expr when source table not provided
	_, err = buildExpr(t, ctx, "default(b)", expression.WithInputSchemaAndNames(schema, names, nil))
	require.EqualError(t, err, "Unsupported expr *ast.DefaultExpr when source table not provided")

	// subquery not supported
	_, err = buildExpr(t, ctx, "a + (select b from t)", expression.WithTableInfo("", tbl))
	require.EqualError(t, err, "planCtx is required when rewriting node: '*ast.SubqueryExpr'")

	// system variables are not supported
	_, err = buildExpr(t, ctx, "@@tidb_enable_async_commit")
	require.EqualError(t, err, "planCtx is required when rewriting node: '*ast.VariableExpr', accessing system variable requires plan context")
	_, err = buildExpr(t, ctx, "@@global.tidb_enable_async_commit")
	require.EqualError(t, err, "planCtx is required when rewriting node: '*ast.VariableExpr', accessing system variable requires plan context")
}
