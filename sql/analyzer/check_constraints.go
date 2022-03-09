// Copyright 2021 Dolthub, Inc.
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

package analyzer

import (
	"fmt"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// validateCheckConstraints validates DDL nodes that create table check constraints, such as CREATE TABLE and
// ALTER TABLE statements.
//
// TODO: validateCheckConstraints doesn't currently do any type validation on the check and will allow you to create
//       checks that will never evaluate correctly.
func validateCheckConstraints(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	switch n := n.(type) {
	case *plan.CreateCheck:
		return validateCreateCheckNode(n)
	case *plan.CreateTable:
		return validateCreateTableChecks(ctx, a, n, scope)
	}

	return n, nil
}

func validateCreateTableChecks(ctx *sql.Context, a *Analyzer, n *plan.CreateTable, scope *Scope) (sql.Node, error) {
	columns, err := indexColumns(ctx, a, n, scope)
	if err != nil {
		return nil, err
	}

	plan.InspectExpressions(n, func(e sql.Expression) bool {
		if err != nil {
			return false
		}

		switch e := e.(type) {
		case *expression.Wrapper, nil:
			// column defaults, no need to inspect these
			return false
		default:
			// check expressions, must be validated
			// TODO: would be better to wrap these in something else to be able to identify them better
			err = checkExpressionValid(e)
			if err != nil {
				return false
			}

			switch e := e.(type) {
			case column:
				col := newTableCol(e.Table(), e.Name())
				if _, ok := columns[col]; !ok {
					err = sql.ErrTableColumnNotFound.New(e.Table(), e.Name())
					return false
				}
			}

			return true
		}
	})

	if err != nil {
		return nil, err
	}

	return n, nil
}

func validateCreateCheckNode(ct *plan.CreateCheck) (sql.Node, error) {
	err := checkExpressionValid(ct.Check.Expr)
	if err != nil {
		return nil, err
	}

	return ct, nil
}

func checkExpressionValid(e sql.Expression) error {
	var err error
	sql.Inspect(e, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *function.GetLock, *function.IsUsedLock, *function.IsFreeLock, function.ReleaseAllLocks, *function.ReleaseLock:
			err = sql.ErrInvalidConstraintFunctionNotSupported.New(e.String())
			return false
		case sql.FunctionExpression:
			if ndf, ok := e.(sql.NonDeterministicExpression); ok && ndf.IsNonDeterministic() {
				err = sql.ErrInvalidConstraintFunctionNotSupported.New(e.String())
			}
			return false
		case *plan.Subquery:
			err = sql.ErrInvalidConstraintSubqueryNotSupported.New(e.String())
			return false
		}
		return true
	})
	return err
}

// loadChecks loads any checks that are required for a plan node to operate properly (except for nodes dealing with
// check execution).
func loadChecks(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("loadChecks")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, bool, error) {
		switch node := n.(type) {
		case *plan.InsertInto:
			nn := *node

			rtable := getResolvedTable(nn.Destination)
			if rtable == nil {
				return node, false, nil
			}

			table, ok := rtable.Table.(sql.CheckTable)
			if !ok {
				return node, false, nil
			}

			var err error
			nn.Checks, err = loadChecksFromTable(ctx, table)
			if err != nil {
				return nil, false, err
			}
			if len(nn.Checks) == 0 {
				return node, false, nil
			}
			return &nn, true, nil
		case *plan.Update:
			nn := *node

			rtable := getResolvedTable(nn.Child)
			if rtable == nil {
				return node, false, nil
			}

			table, ok := rtable.Table.(sql.CheckTable)
			if !ok {
				return node, false, nil
			}

			var err error
			nn.Checks, err = loadChecksFromTable(ctx, table)
			if err != nil {
				return nil, false, err
			}
			if len(nn.Checks) == 0 {
				return node, false, nil
			}
			return &nn, true, nil
		case *plan.ShowCreateTable:
			nn := *node

			rtable := getResolvedTable(nn.Child)
			if rtable == nil {
				return node, false, nil
			}

			table, ok := rtable.Table.(sql.CheckTable)
			if !ok {
				return node, false, nil
			}

			var err error
			checks, err := loadChecksFromTable(ctx, table)
			if err != nil {
				return nil, false, err
			}

			if len(checks) == 0 {
				return node, false, nil
			}

			// To match MySQL output format, transform the column names and wrap with backticks
			transformedChecks := make(sql.CheckConstraints, len(checks))
			for i, check := range checks {
				newExpr, err := expression.TransformUp(check.Expr, func(e sql.Expression) (sql.Expression, bool, error) {
					if t, ok := e.(*expression.UnresolvedColumn); ok {
						return expression.NewUnresolvedColumn(fmt.Sprintf("`%s`", t.Name())), true, nil
					}

					return e, false, nil
				})

				if err != nil {
					return nil, false, err
				}

				check.Expr = newExpr
				transformedChecks[i] = check
			}
			nn.Checks = transformedChecks

			return &nn, true, nil

		case *plan.DropColumn:
			nn := *node

			rtable := getResolvedTable(nn.Child)
			if rtable == nil {
				return node, false, nil
			}

			table, ok := rtable.Table.(sql.CheckTable)
			if !ok {
				return node, false, nil
			}

			var err error
			nn.Checks, err = loadChecksFromTable(ctx, table)
			if err != nil {
				return nil, false, err
			}

			if len(nn.Checks) == 0 {
				return node, false, nil
			}

			return &nn, true, nil

		case *plan.RenameColumn:
			nn := *node

			rtable := getResolvedTable(nn.Child)
			if rtable == nil {
				return node, false, nil
			}

			table, ok := rtable.Table.(sql.CheckTable)
			if !ok {
				return node, false, nil
			}

			var err error
			nn.Checks, err = loadChecksFromTable(ctx, table)
			if err != nil {
				return nil, false, err
			}

			if len(nn.Checks) == 0 {
				return node, false, nil
			}

			return &nn, true, nil

		// TODO: ModifyColumn can also invalidate table check constraints (e.g. by changing the column's type).
		//       Ideally, we should also load checks for ModifyColumn and error out if they would be invalidated.
		// case *plan.ModifyColumn:

		default:
			return node, false, nil
		}
	})
}

func loadChecksFromTable(ctx *sql.Context, table sql.Table) ([]*sql.CheckConstraint, error) {
	var loadedChecks []*sql.CheckConstraint
	if checkTable, ok := table.(sql.CheckTable); ok {
		checks, err := checkTable.GetChecks(ctx)
		if err != nil {
			return nil, err
		}
		for _, ch := range checks {
			constraint, err := convertCheckDefToConstraint(ctx, &ch)
			if err != nil {
				return nil, err
			}
			loadedChecks = append(loadedChecks, constraint)
		}
	}
	return loadedChecks, nil
}

func convertCheckDefToConstraint(ctx *sql.Context, check *sql.CheckDefinition) (*sql.CheckConstraint, error) {
	parseStr := fmt.Sprintf("select %s", check.CheckExpression)
	parsed, err := sqlparser.Parse(parseStr)
	if err != nil {
		return nil, err
	}

	selectStmt, ok := parsed.(*sqlparser.Select)
	if !ok || len(selectStmt.SelectExprs) != 1 {
		return nil, sql.ErrInvalidCheckConstraint.New(check.CheckExpression)
	}

	expr := selectStmt.SelectExprs[0]
	ae, ok := expr.(*sqlparser.AliasedExpr)
	if !ok {
		return nil, sql.ErrInvalidCheckConstraint.New(check.CheckExpression)
	}

	c, err := parse.ExprToExpression(ctx, ae.Expr)
	if err != nil {
		return nil, err
	}

	return &sql.CheckConstraint{
		Name:     check.Name,
		Expr:     c,
		Enforced: check.Enforced,
	}, nil
}
