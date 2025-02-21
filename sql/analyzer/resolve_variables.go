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
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// resolveVariables replaces UnresolvedColumn which are variables with their literal values
func resolveVariables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_variables")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if n.Resolved() {
			return n, nil
		}

		resolveVars := func(e sql.Expression) (sql.Expression, error) {
			uc, ok := e.(column)
			if !ok || e.Resolved() {
				return e, nil
			}

			expr, ok, err := resolveSystemOrUserVariable(ctx, a, uc)
			if err != nil {
				return nil, err
			}
			if ok {
				return expr, nil
			}

			return e, nil
		}

		// Set nodes need to resolve the right-hand side of an expression only
		if n, ok := n.(*plan.Set); ok {
			return plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
				sf, ok := e.(*expression.SetField)
				if !ok {
					return e, nil
				}

				nr, err := expression.TransformUp(sf.Right, resolveVars)
				if err != nil {
					return nil, err
				}

				return sf.WithChildren(sf.Left, nr)
			})
		}

		return plan.TransformExpressionsUp(n, resolveVars)
	})
}

// resolveSetVariables replaces SET @@var and SET @var expressions with appropriately resolved expressions for the
// left-hand side, and evaluate the right-hand side where possible, including filling in defaults. Also validates that
// system variables are known to the system.
func resolveSetVariables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		_, ok := n.(*plan.Set)
		if !ok || n.Resolved() {
			return n, nil
		}

		return plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
			sf, ok := e.(*expression.SetField)
			if !ok {
				return e, nil
			}

			setExpr := sf.Left
			varName := sf.Left.String()
			setVal, err := getSetVal(ctx, varName, sf.Right)
			if err != nil {
				return nil, err
			}

			if _, ok := sf.Left.(*expression.UnresolvedColumn); ok {
				var scope sqlparser.SetScope
				varName, scope, err = sqlparser.VarScope(varName)
				if err != nil {
					return nil, err
				}

				switch scope {
				case sqlparser.SetScope_None:
					return sf, nil
				case sqlparser.SetScope_Global:
					_, _, ok = sql.SystemVariables.GetGlobal(varName)
					if !ok {
						return nil, sql.ErrUnknownSystemVariable.New(varName)
					}
					setExpr = expression.NewSystemVar(varName, sql.SystemVariableScope_Global)
				case sqlparser.SetScope_Persist:
					return nil, sql.ErrUnsupportedFeature.New("PERSIST")
				case sqlparser.SetScope_PersistOnly:
					return nil, sql.ErrUnsupportedFeature.New("PERSIST_ONLY")
				case sqlparser.SetScope_Session:
					_, err = ctx.GetSessionVariable(ctx, varName)
					if err != nil {
						return nil, err
					}
					setExpr = expression.NewSystemVar(varName, sql.SystemVariableScope_Session)
				case sqlparser.SetScope_User:
					setExpr = expression.NewUserVar(varName)
				default: // shouldn't happen
					return nil, fmt.Errorf("unknown set scope %v", scope)
				}
			}

			// Special case: for system variables, MySQL allows naked strings (without quotes), which get interpreted as
			// unresolved columns.
			if _, ok := setExpr.(*expression.SystemVar); ok {
				if uc, ok := setVal.(*expression.UnresolvedColumn); ok && uc.Table() == "" {
					_, setScope, _ := sqlparser.VarScope(uc.Name())
					if setScope == sqlparser.SetScope_None {
						setVal = expression.NewLiteral(uc.Name(), sql.LongText)
					}
				}
			}
			if _, ok := setExpr.(*expression.SystemVar); ok {
				return sf.WithChildren(setExpr, setVal)
			} else if _, ok := setExpr.(*expression.UserVar); ok {
				return sf.WithChildren(setExpr, setVal)
			}
			return sf, nil
		})
	})
}

// resolveUnquotedSetVariables does a similar pass as resolveSetVariables, but handles system vars that were provided
// as barewords (vars not prefixed with @@, and string values unquoted). These will have been deferred into
// deferredColumns by the resolve_columns rule.
func resolveBarewordSetVariables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	_, ok := n.(*plan.Set)
	if !ok || n.Resolved() {
		return n, nil
	}

	return plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
		sf, ok := e.(*expression.SetField)
		if !ok {
			return e, nil
		}

		setVal, err := getSetVal(ctx, sf.Left.String(), sf.Right)
		if err != nil {
			return nil, err
		}

		// If this column expression was deferred, it means that it wasn't prefixed with @@ and can't be found in any table.
		// So treat it as a naked system variable and see if it exists
		if uc, ok := sf.Left.(*deferredColumn); ok {
			varName := uc.String()
			_, _, ok = sql.SystemVariables.GetGlobal(varName)
			if !ok {
				return sf, nil
			}

			// Special case: for system variables, MySQL allows naked strings (without quotes), which get interpreted as
			// unresolved columns.
			if uc, ok := setVal.(column); ok && uc.Table() == "" {
				_, setScope, _ := sqlparser.VarScope(uc.Name())
				if setScope == sqlparser.SetScope_None {
					setVal = expression.NewLiteral(uc.Name(), sql.LongText)
				}
			}

			return sf.WithChildren(expression.NewSystemVar(varName, sql.SystemVariableScope_Session), setVal)
		}

		return sf, nil
	})
}

func resolveSystemOrUserVariable(ctx *sql.Context, a *Analyzer, col column) (sql.Expression, bool, error) {
	var varName string
	var scope sqlparser.SetScope
	var err error
	if col.Table() != "" {
		varName, scope, err = sqlparser.VarScope(col.Table(), col.Name())
		if err != nil {
			return nil, false, err
		}
	} else {
		varName, scope, err = sqlparser.VarScope(col.Name())
		if err != nil {
			return nil, false, err
		}
	}
	switch scope {
	case sqlparser.SetScope_None:
		return nil, false, nil
	case sqlparser.SetScope_Global:
		_, _, ok := sql.SystemVariables.GetGlobal(varName)
		if !ok {
			return nil, false, sql.ErrUnknownSystemVariable.New(varName)
		}
		a.Log("resolved column %s to global system variable", col)
		return expression.NewSystemVar(varName, sql.SystemVariableScope_Global), true, nil
	case sqlparser.SetScope_Persist:
		return nil, false, sql.ErrUnsupportedFeature.New("PERSIST")
	case sqlparser.SetScope_PersistOnly:
		return nil, false, sql.ErrUnsupportedFeature.New("PERSIST_ONLY")
	case sqlparser.SetScope_Session:
		_, err = ctx.GetSessionVariable(ctx, varName)
		if err != nil {
			return nil, false, err
		}
		a.Log("resolved column %s to session system variable", col)
		return expression.NewSystemVar(varName, sql.SystemVariableScope_Session), true, nil
	case sqlparser.SetScope_User:
		t, _, err := ctx.GetUserVariable(ctx, varName)
		if err != nil {
			return nil, false, err
		}
		a.Log("resolved column %s to user variable", col)
		return expression.NewUserVarWithType(varName, t), true, nil
	default: // shouldn't happen
		return nil, false, fmt.Errorf("unknown set scope %v", scope)
	}
}

// getSetVal evaluates the right hand side of a SetField expression and returns an evaluated value as appropriate
func getSetVal(ctx *sql.Context, varName string, e sql.Expression) (sql.Expression, error) {
	if _, ok := e.(*expression.DefaultColumn); ok {
		varName, scope, err := sqlparser.VarScope(varName)
		if err != nil {
			return nil, err
		}
		switch scope {
		case sqlparser.SetScope_None, sqlparser.SetScope_Session, sqlparser.SetScope_Global:
			_, value, ok := sql.SystemVariables.GetGlobal(varName)
			if !ok {
				return nil, sql.ErrUnknownSystemVariable.New(varName)
			}
			return expression.NewLiteral(value, sql.ApproximateTypeFromValue(value)), nil
		case sqlparser.SetScope_Persist:
			return nil, sql.ErrUnsupportedFeature.New("PERSIST")
		case sqlparser.SetScope_PersistOnly:
			return nil, sql.ErrUnsupportedFeature.New("PERSIST_ONLY")
		case sqlparser.SetScope_User:
			return nil, sql.ErrUserVariableNoDefault.New(varName)
		default: // shouldn't happen
			return nil, fmt.Errorf("unknown set scope %v", scope)
		}
	}

	if !e.Resolved() {
		return e, nil
	}

	if sql.IsTextOnly(e.Type()) {
		txt, err := e.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}

		val, ok := txt.(string)
		if !ok {
			// TODO: better error message
			return nil, sql.ErrUnsupportedFeature.New("invalid set variable")
		}

		switch strings.ToLower(val) {
		case sqlparser.KeywordString(sqlparser.ON):
			return expression.NewLiteral(true, sql.Boolean), nil
		case sqlparser.KeywordString(sqlparser.TRUE):
			return expression.NewLiteral(true, sql.Boolean), nil
		case sqlparser.KeywordString(sqlparser.OFF):
			return expression.NewLiteral(false, sql.Boolean), nil
		case sqlparser.KeywordString(sqlparser.FALSE):
			return expression.NewLiteral(false, sql.Boolean), nil
		}
	} else if e.Type() == sql.Boolean {
		val, err := e.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}

		b, ok := val.(bool)
		if !ok {
			return e, nil
		}

		if b {
			return expression.NewLiteral(1, sql.Boolean), nil
		} else {
			return expression.NewLiteral(0, sql.Boolean), nil
		}
	}

	return e, nil
}
