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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func applyHashIn(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUp(n, func(node sql.Node) (sql.Node, bool, error) {
		filter, ok := node.(*plan.Filter)
		if !ok {
			return node, false, nil
		}

		e, err := expression.TransformUp(filter.Expression, func(expr sql.Expression) (sql.Expression, bool, error) {
			if e, ok := expr.(*expression.InTuple); ok &&
				hasSingleOutput(e.Left()) &&
				isStatic(e.Right()) {
				newe, err := expression.NewHashInTuple(e.Left(), e.Right())
				if err != nil {
					return nil, false, err
				}
				return newe, true, nil
			}
			return expr, false, nil
		})
		if err != nil {
			return nil, false, err
		}
		node, err = filter.WithExpressions(e)
		if err != nil {
			return nil, false, err
		}
		return node, true, nil
	})
}

// hasSingleOutput checks if an expression evaluates to a single output
func hasSingleOutput(e sql.Expression) bool {
	return !expression.InspectUp(e, func(expr sql.Expression) bool {
		switch expr.(type) {
		case expression.Tuple, *expression.Literal, *expression.GetField,
			expression.Comparer, *expression.Convert, sql.FunctionExpression,
			*expression.IsTrue, *expression.IsNull, *expression.Arithmetic:
			return false
		default:
			return true
		}
		return false
	})
}

// isStatic checks if an expression is static
func isStatic(e sql.Expression) bool {
	return !expression.InspectUp(e, func(expr sql.Expression) bool {
		switch expr.(type) {
		case expression.Tuple, *expression.Literal:
			return false
		default:
			return true
		}
	})
}
