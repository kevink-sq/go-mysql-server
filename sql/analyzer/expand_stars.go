// Copyright 2020-2021 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// expandStars replaces star expressions into lists of concrete column expressions
func expandStars(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("expand_stars")
	defer span.Finish()

	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, err
	}

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, bool, error) {
		if n.Resolved() {
			return n, false, nil
		}

		switch n := n.(type) {
		case *plan.Project:
			if !n.Child.Resolved() {
				return n, false, nil
			}

			expanded, mod, err := expandStarsForExpressions(a, n.Projections, n.Child.Schema(), tableAliases)
			if err != nil {
				return nil, false, err
			}
			if !mod {
				return n, false, nil
			}
			return plan.NewProject(expanded, n.Child), true, nil
		case *plan.GroupBy:
			if !n.Child.Resolved() {
				return n, false, nil
			}

			expanded, mod, err := expandStarsForExpressions(a, n.SelectedExprs, n.Child.Schema(), tableAliases)
			if err != nil {
				return nil, false, err
			}
			if !mod {
				return n, false, nil
			}
			return plan.NewGroupBy(expanded, n.GroupByExprs, n.Child), true, nil
		case *plan.Window:
			if !n.Child.Resolved() {
				return n, false, nil
			}
			expanded, mod, err := expandStarsForExpressions(a, n.SelectExprs, n.Child.Schema(), tableAliases)
			if err != nil {
				return nil, false, err
			}
			if !mod {
				return n, false, nil
			}
			return plan.NewWindow(expanded, n.Child), true, nil
		default:
			return n, false, nil
		}
	})
}

func expandStarsForExpressions(a *Analyzer, exprs []sql.Expression, schema sql.Schema, tableAliases TableAliases) ([]sql.Expression, bool, error) {
	var expressions []sql.Expression
	var found bool
	for _, e := range exprs {
		if star, ok := e.(*expression.Star); ok {
			found = true
			var exprs []sql.Expression
			for i, col := range schema {
				lowerSource := strings.ToLower(col.Source)
				lowerTable := strings.ToLower(star.Table)
				if star.Table == "" || lowerTable == lowerSource {
					exprs = append(exprs, expression.NewGetFieldWithTable(
						i, col.Type, col.Source, col.Name, col.Nullable,
					))
				}
			}

			if len(exprs) == 0 && star.Table != "" {
				return nil, false, sql.ErrTableNotFound.New(star.Table)
			}

			expressions = append(expressions, exprs...)
		} else {
			expressions = append(expressions, e)
		}
	}

	a.Log("resolved * to expressions %s", expressions)
	return expressions, found, nil
}
