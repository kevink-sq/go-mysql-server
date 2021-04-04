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

package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
)

// Provider resolves SQL catalogs
type Provider interface {
	Resolve(name string) (string, *sql.Catalog, error)
}

// New returns a driver using the specified provider.
func New(provider Provider) *Driver {
	return &Driver{
		provider: provider,
	}
}

// Driver exposes an engine as a stdlib SQL driver.
type Driver struct {
	provider Provider

	mu       sync.Mutex
	catalogs map[*sql.Catalog]*catalog
}

// Open returns a new connection to the database.
func (d *Driver) Open(name string) (driver.Conn, error) {
	conn, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return conn.Connect(context.Background())
}

// OpenConnector calls the driver factory and returns a new connector.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	server, sqlCat, err := d.provider.Resolve(name)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	cat, ok := d.catalogs[sqlCat]
	if !ok {
		anlz := analyzer.NewDefault(sqlCat)
		engine := sqle.New(sqlCat, anlz, nil)
		cat = &catalog{engine: engine}
		if d.catalogs == nil {
			d.catalogs = map[*sql.Catalog]*catalog{}
		}
		d.catalogs[sqlCat] = cat
	}
	d.mu.Unlock()

	return &Connector{
		driver:  d,
		server:  server,
		catalog: cat,
	}, nil
}

type catalog struct {
	engine *sqle.Engine

	mu     sync.Mutex
	connID uint32
	procID uint64
}

func (c *catalog) nextConnectionID() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connID++
	return c.connID
}

func (c *catalog) nextProcessID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.procID++
	return c.procID
}

// A Connector represents a driver in a fixed configuration
// and can create any number of equivalent Conns for use
// by multiple goroutines.
type Connector struct {
	driver  *Driver
	server  string
	catalog *catalog
}

// Driver returns the driver.
func (c *Connector) Driver() driver.Driver {
	return c.driver
}

// Connect returns a connection to the database.
func (c *Connector) Connect(context.Context) (driver.Conn, error) {
	id := c.catalog.nextConnectionID()

	session := sql.NewSession(c.server, fmt.Sprintf("#%d", id), "", id)
	indexes := sql.NewIndexRegistry()
	views := sql.NewViewRegistry()
	return &Conn{
		catalog: c.catalog,
		session: session,
		indexes: indexes,
		views:   views,
	}, nil
}
