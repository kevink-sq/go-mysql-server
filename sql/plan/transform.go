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

package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// TransformContext is the parameter to the Transform{,Selector}.
type TransformContext struct {
	// Node is the currently visited node which will be transformed.
	Node sql.Node
	// Parent is the current parent of the transforming node.
	Parent sql.Node
	// ChildNum is the index of Node in Parent.Children().
	ChildNum int
	// SchemaPrefix is the concatenation of the Parent's SchemaPrefix with
	// child.Schema() for all child with an index < ChildNum in
	// Parent.Children(). For many Nodes, this represents the schema of the
	// |row| parameter that is going to be passed to this node by its
	// parent in a RowIter() call. This field is only non-nil if the entire
	// in-order traversal of the tree up to this point is Resolved().
	SchemaPrefix sql.Schema
}

type SchemaPrefixer struct {
	// schema is just a slice of columns
	// allocate upfront
	// can switch out slice when one is changed?
	cols []*sql.Column
	i    int
}

// Transformer is a function which will return new sql.Node values for a given
// TransformContext.
type Transformer func(TransformContext) (sql.Node, bool, error)

// TransformSelector is a function which will allow TransformUpCtx to not
// traverse past a certain TransformContext. If this function returns |false|
// for a given TransformContext, the subtree is not transformed and the child
// is kept in its existing place in the parent as-is.
type TransformSelector func(TransformContext) bool

// TransformExpressionsUpWithNode applies a transformation function to all expressions
// on the given tree from the bottom up.
func TransformExpressionsUpWithNode(node sql.Node, f expression.TransformExprWithNodeFunc) (sql.Node, error) {
	return TransformUp(node, func(n sql.Node) (sql.Node, bool, error) {
		return TransformExpressionsWithNode(n, f)
	})
}

// TransformExpressionsUp applies a transformation function to all expressions
// on the given tree from the bottom up.
func TransformExpressionsUp(node sql.Node, f sql.TransformExprFunc) (sql.Node, error) {
	return TransformExpressionsUpWithNode(node, func(n sql.Node, e sql.Expression) (sql.Expression, bool, error) {
		return f(e)
	})
}

// TransformExpressionsWithNode applies a transformation function to all expressions
// on the given node.
func TransformExpressionsWithNode(n sql.Node, f expression.TransformExprWithNodeFunc) (sql.Node, bool, error) {
	e, ok := n.(sql.Expressioner)
	if !ok {
		return n, false, nil
	}

	exprs := e.Expressions()
	if len(exprs) == 0 {
		return n, false, nil
	}

	var newExprs []sql.Expression
	var mod bool
	var err error
	for i, e := range exprs {
		e, mod, err = expression.TransformUpWithNode(n, e, f)
		if err != nil {
			return nil, false, err
		}
		if mod {
			if newExprs == nil {
				newExprs = make([]sql.Expression, len(exprs))
				copy(newExprs, exprs)
			}
			newExprs[i] = e
		}
	}

	if len(newExprs) > 0 {
		n, err = e.WithExpressions(newExprs...)
		if err != nil {
			return nil, false, err
		}
		return n, true, nil
	}
	return n, false, nil
}

// TransformExpressionsForNode applies a transformation function to all expressions
// on the given node.
func TransformExpressionsForNode(n sql.Node, f sql.TransformExprFunc) (sql.Node, bool, error) {
	e, ok := n.(sql.Expressioner)
	if !ok {
		return n, false, nil
	}

	exprs := e.Expressions()
	if len(exprs) == 0 {
		return n, false, nil
	}

	var newExprs []sql.Expression
	var modC bool
	var err error
	var expr sql.Expression
	for i := 0; i < len(exprs); i++ {
		expr = exprs[i]
		expr, modC, err = expression.TransformUpHelper(expr, f)
		if err != nil {
			return nil, false, err
		}
		if modC {
			if newExprs == nil {
				newExprs = make([]sql.Expression, len(exprs))
				copy(newExprs, exprs)
			}
			newExprs[i] = expr
		}
	}
	if len(newExprs) > 0 {
		n, err = e.WithExpressions(newExprs...)
		if err != nil {
			return nil, false, err
		}
		return n, true, nil
	}
	return n, false, nil
}

// TransformUpCtx transforms |n| from the bottom up, left to right, by passing
// each node to |f|. If |s| is non-nil, does not descend into children where
// |s| returns false.
func TransformUpCtx(n sql.Node, s TransformSelector, f Transformer) (sql.Node, error) {
	newn, mod, err := TransformUpCtxHelper(TransformContext{n, nil, -1, sql.Schema{}}, s, f)
	if mod {
		return newn, err
	}
	return n, err
}

func TransformUpCtxHelper(c TransformContext, s TransformSelector, f Transformer) (sql.Node, bool, error) {
	node := c.Node
	_, ok := node.(sql.OpaqueNode)
	if ok {
		return f(c)
	}

	children := node.Children()
	if len(children) == 0 {
		return f(c)
	}

	var (
		newChildren []sql.Node
		modC        bool
		err         error
		child       sql.Node
		cc          TransformContext
	)

	for i := 0; i < len(children); i++ {
		child = children[i]
		cc = TransformContext{child, node, i, nil}
		if s == nil || s(cc) {
			child, modC, err = TransformUpCtxHelper(cc, s, f)
			if err != nil {
				return nil, false, err
			}
			if modC {
				if newChildren == nil {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = child
			}
		}
	}

	modC = len(newChildren) > 0
	if modC {
		node, err = node.WithChildren(newChildren...)
		if err != nil {
			return nil, false, err
		}
	}

	node, modN, err := f(TransformContext{node, c.Parent, c.ChildNum, c.SchemaPrefix})
	if err != nil {
		return nil, false, err
	}
	return node, modC || modN, nil
}

// TransformUpWithPrefixSchema transforms |n| from the bottom up, left to right, by passing
// each node to |f|. If |s| is non-nil, does not descend into children where
// |s| returns false.
func TransformUpWithPrefixSchema(n sql.Node, s TransformSelector, f Transformer) (sql.Node, error) {
	newn, mod, err := transformUpWithPrefixSchemaHelper(TransformContext{n, nil, -1, sql.Schema{}}, s, f)
	if mod {
		return newn, err
	}
	return n, err
}

func transformUpWithPrefixSchemaHelper(c TransformContext, s TransformSelector, f Transformer) (sql.Node, bool, error) {
	node := c.Node
	_, ok := node.(sql.OpaqueNode)
	if ok {
		return f(c)
	}

	children := node.Children()
	if len(children) == 0 {
		return f(c)
	}

	var newChildren []sql.Node
	var mod bool
	var err error
	var child sql.Node
	var cc TransformContext
	childPrefix := append(sql.Schema{}, c.SchemaPrefix...)
	for i := 0; i < len(children); i++ {
		child = children[i]
		cc = TransformContext{child, node, i, childPrefix}
		if s == nil || s(cc) {
			child, mod, err = transformUpWithPrefixSchemaHelper(cc, s, f)
			if err != nil {
				return nil, false, err
			}
			if mod {
				if newChildren == nil {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = child
			}
			if child.Resolved() && childPrefix != nil {
				cs := child.Schema()
				childPrefix = append(childPrefix, cs...)
			} else {
				childPrefix = nil
			}
		}
	}

	mod = len(newChildren) > 0
	if mod {
		node, err = node.WithChildren(newChildren...)
		if err != nil {
			return nil, false, err
		}
	}

	node, ok, err = f(TransformContext{node, c.Parent, c.ChildNum, c.SchemaPrefix})
	if err != nil {
		return nil, false, err
	}
	return node, mod || ok, nil
}

// TransformUp applies a transformation function to the given tree from the
// bottom up.
func TransformUp(node sql.Node, f sql.TransformNodeFunc) (sql.Node, error) {
	newn, _, err := TransformUpHelper(node, f)
	return newn, err
}

func TransformUpHelper(node sql.Node, f sql.TransformNodeFunc) (sql.Node, bool, error) {
	_, ok := node.(sql.OpaqueNode)
	if ok {
		return f(node)
	}

	children := node.Children()
	if len(children) == 0 {
		return f(node)
	}

	var newChildren []sql.Node
	var modC bool
	var err error
	var child sql.Node
	for i := 0; i < len(children); i++ {
		child = children[i]
		child, modC, err = TransformUpHelper(child, f)
		if err != nil {
			return nil, false, err
		}
		if modC {
			if newChildren == nil {
				newChildren = make([]sql.Node, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = child
		}
	}

	modC = len(newChildren) > 0
	if modC {
		node, err = node.WithChildren(newChildren...)
		if err != nil {
			return nil, false, err
		}
	}

	node, modN, err := f(node)
	if err != nil {
		return nil, false, err
	}
	return node, modC || modN, nil
}
