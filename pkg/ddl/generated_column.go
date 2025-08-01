// Copyright 2017 PingCAP, Inc.
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

package ddl

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// columnGenerationInDDL is a struct for validating generated columns in DDL.
type columnGenerationInDDL struct {
	position    int
	generated   bool
	dependences map[string]struct{}
}

// verifyColumnGeneration is for CREATE TABLE, because we need verify all columns in the table.
func verifyColumnGeneration(colName2Generation map[string]columnGenerationInDDL, colName string) error {
	attribute := colName2Generation[colName]
	if attribute.generated {
		for depCol := range attribute.dependences {
			attr, ok := colName2Generation[depCol]
			if !ok {
				err := dbterror.ErrBadField.GenWithStackByArgs(depCol, "generated column function")
				return errors.Trace(err)
			}
			if attr.generated && attribute.position <= attr.position {
				// A generated column definition can refer to other
				// generated columns occurring earlier in the table.
				err := dbterror.ErrGeneratedColumnNonPrior.GenWithStackByArgs()
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// verifyColumnGenerationSingle is for ADD GENERATED COLUMN, we just need verify one column itself.
func verifyColumnGenerationSingle(dependColNames map[string]struct{}, cols []*table.Column, position *ast.ColumnPosition) error {
	// Since the added column does not exist yet, we should derive it's offset from ColumnPosition.
	pos, err := findPositionRelativeColumn(cols, position)
	if err != nil {
		return errors.Trace(err)
	}
	// should check unknown column first, then the prior ones.
	for _, col := range cols {
		if _, ok := dependColNames[col.Name.L]; ok {
			if col.IsGenerated() && col.Offset >= pos {
				// Generated column can refer only to generated columns defined prior to it.
				return dbterror.ErrGeneratedColumnNonPrior.GenWithStackByArgs()
			}
		}
	}
	return nil
}

// checkDependedColExist ensure all depended columns exist and not hidden.
// NOTE: this will MODIFY parameter `dependCols`.
func checkDependedColExist(dependCols map[string]struct{}, cols []*table.Column) error {
	for _, col := range cols {
		if !col.Hidden {
			delete(dependCols, col.Name.L)
		}
	}
	if len(dependCols) != 0 {
		for arbitraryCol := range dependCols {
			return dbterror.ErrBadField.GenWithStackByArgs(arbitraryCol, "generated column function")
		}
	}
	return nil
}

// findPositionRelativeColumn returns a pos relative to added generated column position.
func findPositionRelativeColumn(cols []*table.Column, pos *ast.ColumnPosition) (int, error) {
	position := len(cols)
	// Get the column position, default is cols's length means appending.
	// For "alter table ... add column(...)", the position will be nil.
	// For "alter table ... add column ... ", the position will be default one.
	if pos == nil {
		return position, nil
	}
	if pos.Tp == ast.ColumnPositionFirst {
		position = 0
	} else if pos.Tp == ast.ColumnPositionAfter {
		var col *table.Column
		for _, c := range cols {
			if c.Name.L == pos.RelativeColumn.Name.L {
				col = c
				break
			}
		}
		if col == nil {
			return -1, dbterror.ErrBadField.GenWithStackByArgs(pos.RelativeColumn, "generated column function")
		}
		// Inserted position is after the mentioned column.
		position = col.Offset + 1
	}
	return position, nil
}

// findDependedColumnNames returns a set of string, which indicates
// the names of the columns that are depended by colDef.
func findDependedColumnNames(schemaName ast.CIStr, tableName ast.CIStr, colDef *ast.ColumnDef) (generated bool, colsMap map[string]struct{}, err error) {
	colsMap = make(map[string]struct{})
	for _, option := range colDef.Options {
		if option.Tp == ast.ColumnOptionGenerated {
			generated = true
			colNames := FindColumnNamesInExpr(option.Expr)
			for _, depCol := range colNames {
				if depCol.Schema.L != "" && schemaName.L != "" && depCol.Schema.L != schemaName.L {
					return false, nil, dbterror.ErrWrongDBName.GenWithStackByArgs(depCol.Schema.O)
				}
				if depCol.Table.L != "" && tableName.L != "" && depCol.Table.L != tableName.L {
					return false, nil, dbterror.ErrWrongTableName.GenWithStackByArgs(depCol.Table.O)
				}
				colsMap[depCol.Name.L] = struct{}{}
			}
			break
		}
	}
	return
}

// FindColumnNamesInExpr returns a slice of ast.ColumnName which is referred in expr.
func FindColumnNamesInExpr(expr ast.ExprNode) []*ast.ColumnName {
	var c generatedColumnChecker
	expr.Accept(&c)
	return c.cols
}

// hasDependentByGeneratedColumn checks whether there are other columns depend on this column or not.
func hasDependentByGeneratedColumn(tblInfo *model.TableInfo, colName ast.CIStr) (bool, string, bool) {
	for _, col := range tblInfo.Columns {
		for dep := range col.Dependences {
			if dep == colName.L {
				return true, dep, col.Hidden
			}
		}
	}
	return false, "", false
}

func isGeneratedRelatedColumn(tblInfo *model.TableInfo, newCol, col *model.ColumnInfo) error {
	if newCol.IsGenerated() || col.IsGenerated() {
		// TODO: Make it compatible with MySQL error.
		msg := fmt.Sprintf("newCol IsGenerated %v, oldCol IsGenerated %v", newCol.IsGenerated(), col.IsGenerated())
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}
	if ok, dep, _ := hasDependentByGeneratedColumn(tblInfo, col.Name); ok {
		msg := fmt.Sprintf("oldCol is a dependent column '%s' for generated column", dep)
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}
	return nil
}

type generatedColumnChecker struct {
	cols []*ast.ColumnName
}

func (*generatedColumnChecker) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	return inNode, false
}

func (c *generatedColumnChecker) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	if x, ok := inNode.(*ast.ColumnName); ok {
		c.cols = append(c.cols, x)
	}
	return inNode, true
}

// checkModifyGeneratedColumn checks the modification between
// old and new is valid or not by such rules:
//  1. the modification can't change stored status;
//  2. if the new is generated, check its refer rules.
//  3. check if the modified expr contains non-deterministic functions
//  4. check whether new column refers to any auto-increment columns.
//  5. check if the new column is indexed or stored
func checkModifyGeneratedColumn(sctx sessionctx.Context, schemaName ast.CIStr, tbl table.Table, oldCol, newCol *table.Column, newColDef *ast.ColumnDef, pos *ast.ColumnPosition) error {
	// rule 1.
	oldColIsStored := !oldCol.IsGenerated() || oldCol.GeneratedStored
	newColIsStored := !newCol.IsGenerated() || newCol.GeneratedStored
	if oldColIsStored != newColIsStored {
		return dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Changing the STORED status")
	}

	// rule 2.
	originCols := tbl.Cols()
	var err error
	var colName2Generation = make(map[string]columnGenerationInDDL, len(originCols))
	for i, column := range originCols {
		// We can compare the pointers simply.
		if column == oldCol {
			if pos != nil && pos.Tp != ast.ColumnPositionNone {
				i, err = findPositionRelativeColumn(originCols, pos)
				if err != nil {
					return errors.Trace(err)
				}
			}
			colName2Generation[newCol.Name.L] = columnGenerationInDDL{
				position:    i,
				generated:   newCol.IsGenerated(),
				dependences: newCol.Dependences,
			}
		} else if !column.IsGenerated() {
			colName2Generation[column.Name.L] = columnGenerationInDDL{
				position:  i,
				generated: false,
			}
		} else {
			colName2Generation[column.Name.L] = columnGenerationInDDL{
				position:    i,
				generated:   true,
				dependences: column.Dependences,
			}
		}
	}
	// We always need test all columns, even if it's not changed
	// because other can depend on it so its name can't be changed.
	for _, column := range originCols {
		var colName string
		if column == oldCol {
			colName = newCol.Name.L
		} else {
			colName = column.Name.L
		}
		if err := verifyColumnGeneration(colName2Generation, colName); err != nil {
			return errors.Trace(err)
		}
	}

	if newCol.IsGenerated() {
		// rule 3.
		if err := checkIllegalFn4Generated(newCol.Name.L, typeColumn, newCol.GeneratedExpr.Internal()); err != nil {
			return errors.Trace(err)
		}

		// rule 4.
		_, dependColNames, err := findDependedColumnNames(schemaName, tbl.Meta().Name, newColDef)
		if err != nil {
			return errors.Trace(err)
		}
		//nolint:forbidigo
		if !sctx.GetSessionVars().EnableAutoIncrementInGenerated {
			if err := checkAutoIncrementRef(newColDef.Name.Name.L, dependColNames, tbl.Meta()); err != nil {
				return errors.Trace(err)
			}
		}

		// rule 5.
		if err := checkIndexOrStored(tbl, oldCol, newCol); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type illegalFunctionChecker struct {
	hasIllegalFunc        bool
	hasAggFunc            bool
	hasRowVal             bool // hasRowVal checks whether the functional index refers to a row value
	hasWindowFunc         bool
	hasNotGAFunc4ExprIdx  bool
	hasCastArrayFunc      bool
	disallowCastArrayFunc bool
	otherErr              error
}

func (c *illegalFunctionChecker) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	switch node := inNode.(type) {
	case *ast.FuncCallExpr:
		// Grouping function is not allowed, issue #49909.
		if node.FnName.L == ast.Grouping {
			c.hasAggFunc = true
			return inNode, true
		}
		// Blocked functions & non-builtin functions is not allowed
		_, isFunctionBlocked := expression.IllegalFunctions4GeneratedColumns[node.FnName.L]
		if isFunctionBlocked || !expression.IsFunctionSupported(node.FnName.L) {
			c.hasIllegalFunc = true
			return inNode, true
		}
		err := expression.VerifyArgsWrapper(node.FnName.L, len(node.Args))
		if err != nil {
			c.otherErr = err
			return inNode, true
		}
		_, isFuncGA := variable.GAFunction4ExpressionIndex[node.FnName.L]
		if !isFuncGA {
			c.hasNotGAFunc4ExprIdx = true
		}
	case *ast.SubqueryExpr, *ast.ValuesExpr, *ast.VariableExpr:
		// Subquery & `values(x)` & variable is not allowed
		c.hasIllegalFunc = true
		return inNode, true
	case *ast.AggregateFuncExpr:
		// Aggregate function is not allowed
		c.hasAggFunc = true
		return inNode, true
	case *ast.RowExpr:
		c.hasRowVal = true
		return inNode, true
	case *ast.WindowFuncExpr:
		c.hasWindowFunc = true
		return inNode, true
	case *ast.FuncCastExpr:
		c.hasCastArrayFunc = c.hasCastArrayFunc || node.Tp.IsArray()
		if c.disallowCastArrayFunc && node.Tp.IsArray() {
			c.otherErr = expression.ErrNotSupportedYet.GenWithStackByArgs("Use of CAST( .. AS .. ARRAY) outside of functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions")
			return inNode, true
		}
	case *ast.ParenthesesExpr:
		return inNode, false
	}
	c.disallowCastArrayFunc = true
	return inNode, false
}

func (*illegalFunctionChecker) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	return inNode, true
}

const (
	typeColumn = iota
	typeIndex
)

func checkIllegalFn4Generated(name string, genType int, expr ast.ExprNode) error {
	if expr == nil {
		return nil
	}
	var c illegalFunctionChecker
	expr.Accept(&c)
	if c.hasIllegalFunc {
		switch genType {
		case typeColumn:
			return dbterror.ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs(name)
		case typeIndex:
			return dbterror.ErrFunctionalIndexFunctionIsNotAllowed.GenWithStackByArgs(name)
		}
	}
	if c.hasAggFunc {
		return dbterror.ErrInvalidGroupFuncUse
	}
	if c.hasRowVal {
		switch genType {
		case typeColumn:
			return dbterror.ErrGeneratedColumnRowValueIsNotAllowed.GenWithStackByArgs(name)
		case typeIndex:
			return dbterror.ErrFunctionalIndexRowValueIsNotAllowed.GenWithStackByArgs(name)
		}
	}
	if c.hasWindowFunc {
		return dbterror.ErrWindowInvalidWindowFuncUse.GenWithStackByArgs(name)
	}
	if c.otherErr != nil {
		return c.otherErr
	}
	if genType == typeIndex && c.hasNotGAFunc4ExprIdx && !config.GetGlobalConfig().Experimental.AllowsExpressionIndex {
		return dbterror.ErrUnsupportedExpressionIndex
	}
	if genType == typeColumn && c.hasCastArrayFunc {
		return expression.ErrNotSupportedYet.GenWithStackByArgs("Use of CAST( .. AS .. ARRAY) outside of functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions")
	}
	return nil
}

func checkIndexOrStored(tbl table.Table, oldCol, newCol *table.Column) error {
	if oldCol.GeneratedExprString == newCol.GeneratedExprString {
		return nil
	}

	if newCol.GeneratedStored {
		return dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("modifying a stored column")
	}

	for _, idx := range tbl.Indices() {
		for _, col := range idx.Meta().Columns {
			if col.Name.L == newCol.Name.L {
				return dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("modifying an indexed column")
			}
		}
	}
	return nil
}

// checkAutoIncrementRef checks if an generated column depends on an auto-increment column and raises an error if so.
// See https://dev.mysql.com/doc/refman/5.7/en/create-table-generated-columns.html for details.
func checkAutoIncrementRef(name string, dependencies map[string]struct{}, tbInfo *model.TableInfo) error {
	exists, autoIncrementColumn := infoschema.HasAutoIncrementColumn(tbInfo)
	if exists {
		if _, found := dependencies[autoIncrementColumn]; found {
			return dbterror.ErrGeneratedColumnRefAutoInc.GenWithStackByArgs(name)
		}
	}
	return nil
}

// checkExpressionIndexAutoIncrement checks if an generated column depends on an auto-increment column and raises an error if so.
func checkExpressionIndexAutoIncrement(name string, dependencies map[string]struct{}, tbInfo *model.TableInfo) error {
	exists, autoIncrementColumn := infoschema.HasAutoIncrementColumn(tbInfo)
	if exists {
		if _, found := dependencies[autoIncrementColumn]; found {
			return dbterror.ErrExpressionIndexCanNotRefer.GenWithStackByArgs(name)
		}
	}
	return nil
}
