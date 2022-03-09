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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// FixFieldIndexesOnExpressions executes FixFieldIndexes on a list of exprs.
func FixFieldIndexesOnExpressions(ctx *sql.Context, scope *Scope, a *Analyzer, schema sql.Schema, expressions ...sql.Expression) ([]sql.Expression, bool, error) {
	var result []sql.Expression
	var res sql.Expression
	var mod bool
	var err error
	for i := 0; i < len(expressions); i++ {
		e := expressions[i]
		res, mod, err = FixFieldIndexes(ctx, scope, a, schema, e)
		if err != nil {
			return nil, false, err
		}
		if mod {
			if result == nil {
				result = make([]sql.Expression, len(expressions))
				copy(result, expressions)
			}
			result[i] = res
		}
	}
	if len(result) > 0 {
		return result, true, nil
	}
	return expressions, false, nil
}

// FixFieldIndexes transforms the given expression by correcting the indexes of columns in GetField expressions,
// according to the schema given. Used when combining multiple tables together into a single join result, or when
// otherwise changing / combining schemas in the node tree.
func FixFieldIndexes(ctx *sql.Context, scope *Scope, a *Analyzer, schema sql.Schema, exp sql.Expression) (sql.Expression, bool, error) {
	scopeLen := len(scope.Schema())

	return expression.TransformUpHelper(exp, func(e sql.Expression) (sql.Expression, bool, error) {
		switch e := e.(type) {
		// For each GetField expression, re-index it with the appropriate index from the schema.
		case *expression.GetField:
			for i, col := range schema {
				newIndex := scopeLen + i
				if e.Name() == col.Name && e.Table() == col.Source {
					if newIndex != e.Index() {
						a.Log("Rewriting field %s.%s from index %d to %d", e.Table(), e.Name(), e.Index(), newIndex)
						return expression.NewGetFieldWithTable(
							newIndex,
							e.Type(),
							e.Table(),
							e.Name(),
							e.IsNullable(),
						), true, nil
					}
					return e, false, nil
				}
			}

			// If we didn't find the column in the schema of the node itself, look outward in surrounding scopes. Work
			// inner-to-outer, in  accordance with MySQL scope naming precedence rules.
			offset := 0
			for _, n := range scope.InnerToOuter() {
				schema := schemas(n.Children())
				offset += len(schema)
				for i, col := range schema {
					if e.Name() == col.Name && e.Table() == col.Source {
						newIndex := scopeLen - offset + i
						if e.Index() != newIndex {
							a.Log("Rewriting field %s.%s from index %d to %d", e.Table(), e.Name(), e.Index(), newIndex)
							return expression.NewGetFieldWithTable(
								newIndex,
								e.Type(),
								e.Table(),
								e.Name(),
								e.IsNullable(),
							), true, nil
						}
						return e, false, nil
					}
				}
			}

			return nil, false, ErrFieldMissing.New(e.Name())
		}

		return e, false, nil
	})
}

// schemas returns the schemas for the nodes given appended in to a single one
func schemas(nodes []sql.Node) sql.Schema {
	var schema sql.Schema
	for _, n := range nodes {
		schema = append(schema, n.Schema()...)
	}
	return schema
}

// Transforms the expressions in the Node given, fixing the field indexes.
func FixFieldIndexesForExpressions(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, bool, error) {
	if _, ok := node.(sql.Expressioner); !ok {
		return node, false, nil
	}

	var schemas []sql.Schema
	for _, child := range node.Children() {
		schemas = append(schemas, child.Schema())
	}

	if len(schemas) < 1 {
		return node, false, nil
	}

	n, modChild, err := plan.TransformExpressionsWithNode(node, func(_ sql.Node, e sql.Expression) (sql.Expression, bool, error) {
		for _, schema := range schemas {
			fixed, mod, err := FixFieldIndexes(ctx, scope, a, schema, e)
			if err == nil {
				return fixed, mod, nil
			}

			if ErrFieldMissing.Is(err) {
				continue
			}

			return nil, false, err
		}

		return e, false, nil
	})

	if err != nil {
		return nil, false, err
	}

	var modJoin bool
	var cond sql.Expression
	switch j := n.(type) {
	case *plan.InnerJoin:
		cond, modJoin, err = FixFieldIndexes(ctx, scope, a, j.Schema(), j.Cond)
		if err != nil {
			return nil, false, err
		}
		if modJoin {
			n, err = j.WithExpressions(cond)
			if err != nil {
				return nil, false, err
			}
		}
	case *plan.RightJoin:
		cond, modJoin, err = FixFieldIndexes(ctx, scope, a, j.Schema(), j.Cond)
		if err != nil {
			return nil, false, err
		}
		if modJoin {
			n, err = j.WithExpressions(cond)
			if err != nil {
				return nil, false, err
			}
		}
	case *plan.LeftJoin:
		cond, modJoin, err = FixFieldIndexes(ctx, scope, a, j.Schema(), j.Cond)
		if err != nil {
			return nil, false, err
		}
		if modJoin {
			n, err = j.WithExpressions(cond)
			if err != nil {
				return nil, false, err
			}
		}
	}

	return n, modChild || modJoin, nil
}

// Transforms the expressions in the Node given, fixing the field indexes. This is useful for Table nodes that have
// expressions but no children.
func FixFieldIndexesForTableNode(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, bool, error) {
	if _, ok := node.(sql.Expressioner); !ok {
		return node, false, nil
	}

	n, mod, err := plan.TransformExpressionsWithNode(node, func(_ sql.Node, e sql.Expression) (sql.Expression, bool, error) {
		schema := node.Schema()
		fixed, mod, err := FixFieldIndexes(ctx, scope, a, schema, e)
		if err != nil {
			if ErrFieldMissing.Is(err) {
				return e, false, nil
			}
			return nil, false, err
		}

		return fixed, mod, nil
	})

	if err != nil {
		return nil, false, err
	}

	return n, mod, nil
}
