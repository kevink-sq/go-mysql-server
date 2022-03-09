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

package expression

import (
	"errors"

	"github.com/dolthub/go-mysql-server/sql"
)

// TransformExprWithNodeFunc is a function that given an expression and the node that contains it, will return that
// expression as is or transformed along with an error, if any.
type TransformExprWithNodeFunc func(sql.Node, sql.Expression) (sql.Expression, bool, error)

func TransformUp(e sql.Expression, f sql.TransformExprFunc) (sql.Expression, error) {
	newe, _, err := TransformUpHelper(e, f)
	return newe, err
}

func TransformUpHelper(e sql.Expression, f sql.TransformExprFunc) (sql.Expression, bool, error) {
	children := e.Children()
	if len(children) == 0 {
		return f(e)
	}

	var modC bool
	var c sql.Expression
	var newChildren []sql.Expression
	var err error
	for i := 0; i < len(children); i++ {
		c = children[i]
		c, modC, err = TransformUpHelper(c, f)
		if err != nil {
			return nil, modC, err
		}
		if modC {
			if newChildren == nil {
				newChildren = make([]sql.Expression, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = c
		}
	}

	modC = len(newChildren) > 0
	if modC {
		e, err = e.WithChildren(newChildren...)
		if err != nil {
			return nil, true, err
		}
	}

	e, modN, err := f(e)
	if err != nil {
		return nil, false, err
	}
	return e, modC || modN, nil
}

// InspectUp traverses the given tree from the bottom up, breaking if
// stop = true. Returns a bool indicating whether traversal was interrupted.
func InspectUp(node sql.Expression, f func(sql.Expression) bool) bool {
	stop := errors.New("stop")
	wrap := func(n sql.Expression) (sql.Expression, bool, error) {
		ok := f(n)
		if ok {
			return nil, false, stop
		}
		return n, false, nil
	}
	_, err := TransformUp(node, func(node sql.Expression) (sql.Expression, bool, error) {
		return wrap(node)
	})
	return errors.Is(err, stop)
}

// Clone duplicates an existing sql.Expression, returning new nodes with the
// same structure and internal values. It can be useful when dealing with
// stateful expression nodes where an evaluation needs to create multiple
// independent histories of the internal state of the expression nodes.
func Clone(expr sql.Expression) (sql.Expression, error) {
	return TransformUp(expr, func(e sql.Expression) (sql.Expression, bool, error) {
		return e, true, nil
	})
}

// TransformUpWithNode applies a transformation function to the given expression from the bottom up.
func TransformUpWithNode(n sql.Node, e sql.Expression, f TransformExprWithNodeFunc) (sql.Expression, bool, error) {
	children := e.Children()
	if len(children) == 0 {
		return f(n, e)
	}

	var newChildren []sql.Expression
	var mod bool
	var err error
	for i, c := range children {
		c, mod, err = TransformUpWithNode(n, c, f)
		if err != nil {
			return nil, false, err
		}
		if mod {
			if newChildren == nil {
				newChildren = make([]sql.Expression, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = c
		}
	}

	mod = len(newChildren) > 0
	if mod {
		e, err = e.WithChildren(newChildren...)
		if err != nil {
			return nil, false, err
		}
	}

	e, ok, err := f(n, e)
	if err != nil {
		return nil, false, err
	}
	return e, mod || ok, nil
}

// ExpressionToColumn converts the expression to the form that should be used in a Schema. Expressions that have Name()
// and Table() methods will use these; otherwise, String() and "" are used, respectively. The type and nullability are
// taken from the expression directly.
func ExpressionToColumn(e sql.Expression) *sql.Column {
	var name string
	if n, ok := e.(sql.Nameable); ok {
		name = n.Name()
	} else {
		name = e.String()
	}

	var table string
	if t, ok := e.(sql.Tableable); ok {
		table = t.Table()
	}

	return &sql.Column{
		Name:     name,
		Type:     e.Type(),
		Nullable: e.IsNullable(),
		Source:   table,
	}
}
