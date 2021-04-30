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
	"fmt"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

var (
	// ErrNoForeignKeySupport is returned when the table does not support FOREIGN KEY operations.
	ErrNoForeignKeySupport = errors.NewKind("the table does not support foreign key operations: %s")
	// ErrForeignKeyMissingColumns is returned when an ALTER TABLE ADD FOREIGN KEY statement does not provide any columns
	ErrForeignKeyMissingColumns = errors.NewKind("cannot create a foreign key without columns")
	// ErrAddForeignKeyDuplicateColumn is returned when an ALTER TABLE ADD FOREIGN KEY statement has the same column multiple times
	ErrAddForeignKeyDuplicateColumn = errors.NewKind("cannot have duplicates of columns in a foreign key: `%v`")
)

type CreateForeignKey struct {
	// In the cases where we have multiple ALTER statements, we need to resolve the table at execution time rather than
	// during analysis. Otherwise, you could add a column in the preceding alter and we may have analyzed to a table
	// that did not yet have that column.
	ddlNode
	Table           string
	ReferencedTable string
	FkDef           *sql.ForeignKeyConstraint
}

var _ sql.Node = (*CreateForeignKey)(nil)
var _ sql.Databaser = (*CreateForeignKey)(nil)

type DropForeignKey struct {
	UnaryNode
	Name string
}

func NewAlterAddForeignKey(db sql.Database, table, refTable string, fkDef *sql.ForeignKeyConstraint) *CreateForeignKey {
	return &CreateForeignKey{
		ddlNode:         ddlNode{db},
		Table:           table,
		ReferencedTable: refTable,
		FkDef:           fkDef,
	}
}

func NewAlterDropForeignKey(table sql.Node, name string) *DropForeignKey {
	return &DropForeignKey{
		UnaryNode: UnaryNode{Child: table},
		Name:      name,
	}
}

func getForeignKeyAlterable(node sql.Node) (sql.ForeignKeyAlterableTable, error) {
	switch node := node.(type) {
	case sql.ForeignKeyAlterableTable:
		return node, nil
	case *ResolvedTable:
		return getForeignKeyAlterableTable(node.Table)
	case sql.TableWrapper:
		return getForeignKeyAlterableTable(node.Underlying())
	}
	for _, child := range node.Children() {
		n, _ := getForeignKeyAlterable(child)
		if n != nil {
			return n, nil
		}
	}
	return nil, ErrNoForeignKeySupport.New(node.String())
}

func getForeignKeyAlterableTable(t sql.Table) (sql.ForeignKeyAlterableTable, error) {
	switch t := t.(type) {
	case sql.ForeignKeyAlterableTable:
		return t, nil
	case sql.TableWrapper:
		return getForeignKeyAlterableTable(t.Underlying())
	default:
		return nil, ErrNoForeignKeySupport.New(t.Name())
	}
}

// Execute inserts the rows in the database.
func (p *CreateForeignKey) Execute(ctx *sql.Context) error {
	tbl, ok, err := p.db.GetTableInsensitive(ctx, p.Table)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrTableNotFound.New(p.Table)
	}
	refTbl, ok, err := p.db.GetTableInsensitive(ctx, p.ReferencedTable)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrTableNotFound.New(p.ReferencedTable)
	}

	fkAlterable, ok := tbl.(sql.ForeignKeyAlterableTable)
	if !ok {
		return ErrNoForeignKeySupport.New(p.Table)
	}

	if len(p.FkDef.Columns) == 0 {
		return ErrForeignKeyMissingColumns.New()
	}

	// Make sure that all columns are valid, in the table, and there are no duplicates
	seenCols := make(map[string]bool)
	for _, col := range fkAlterable.Schema() {
		seenCols[col.Name] = false
	}
	for _, fkCol := range p.FkDef.Columns {
		if seen, ok := seenCols[fkCol]; ok {
			if !seen {
				seenCols[fkCol] = true
			} else {
				return ErrAddForeignKeyDuplicateColumn.New(fkCol)
			}
		} else {
			return sql.ErrTableColumnNotFound.New(fkAlterable.Name(), fkCol)
		}
	}

	// Make sure that the ref columns exist
	for _, refCol := range p.FkDef.ReferencedColumns {
		if !refTbl.Schema().Contains(refCol, p.FkDef.ReferencedTable) {
			return sql.ErrTableColumnNotFound.New(p.FkDef.ReferencedTable, refCol)
		}
	}

	return fkAlterable.CreateForeignKey(ctx, p.FkDef.Name, p.FkDef.Columns, p.FkDef.ReferencedTable, p.FkDef.ReferencedColumns, p.FkDef.OnUpdate, p.FkDef.OnDelete)
}

// WithDatabase implements the sql.Databaser interface.
func (p *CreateForeignKey) WithDatabase(db sql.Database) (sql.Node, error) {
	np := *p
	np.db = db
	return &np, nil
}

// Execute inserts the rows in the database.
func (p *DropForeignKey) Execute(ctx *sql.Context) error {
	fkAlterable, err := getForeignKeyAlterable(p.UnaryNode.Child)
	if err != nil {
		return err
	}

	return fkAlterable.DropForeignKey(ctx, p.Name)
}

// RowIter implements the Node interface.
func (p *DropForeignKey) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

// WithChildren implements the Node interface.
func (p *DropForeignKey) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewAlterDropForeignKey(children[0], p.Name), nil
}

// WithChildren implements the Node interface.
func (p *CreateForeignKey) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(p, children...)
}

func (p *CreateForeignKey) Schema() sql.Schema { return nil }
func (p *DropForeignKey) Schema() sql.Schema   { return nil }

func (p *CreateForeignKey) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (p DropForeignKey) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DropForeignKey(%s)", p.Name)
	_ = pr.WriteChildren(fmt.Sprintf("Table(%s)", p.UnaryNode.Child.String()))
	return pr.String()
}

func (p CreateForeignKey) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("AddForeignKey(%s)", p.FkDef.Name)
	_ = pr.WriteChildren(
		fmt.Sprintf("Table(%s)", p.Table),
		fmt.Sprintf("Columns(%s)", strings.Join(p.FkDef.Columns, ", ")),
		fmt.Sprintf("ReferencedTable(%s)", p.ReferencedTable),
		fmt.Sprintf("ReferencedColumns(%s)", strings.Join(p.FkDef.ReferencedColumns, ", ")),
		fmt.Sprintf("OnUpdate(%s)", p.FkDef.OnUpdate),
		fmt.Sprintf("OnDelete(%s)", p.FkDef.OnDelete))
	return pr.String()
}
