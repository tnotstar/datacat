// Copyright 2023, Antonio Alvarado Hernández <tnotstar@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package sources

import (
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/tnotstar/sqltoapi/core"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
)

// `DatabaseQuerySource` is the concrete implementation of the source interface
// for Oracle databases. It reads data from an Oracle database and sends
// it to the output processing channel.
type DatabaseQuerySource struct {
	// `task` is the name of the task which is running into.
	task string
	// `database` is the name of the database to be used.
	database string
	// `driver` is the name of the database driver to be used.
	driver string
	// `uri` is a string containing the connection URI.
	uri string
	// `query` is a string containing the query to be executed.
	query string
}

// `IsaDatabaseQuerySource` returns true if given source type is
// an Database Query.
func IsaDatabaseQuerySource(sourceType string) bool {
	return sourceType == "database-query-source"
}

// `NewDatabaseQuerySource` creates a new instance of the Database Source endpoint.
//
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
func NewDatabaseQuerySource(cfg core.Configurator, taskName string) core.Source {
	sourceConfig, _ := cfg.GetSourceConfig(taskName)

	dbName := sourceConfig.Arguments["database"].(string)
	dbConfig, err := cfg.GetDatabaseConfig(dbName)
	if err != nil {
		log.Fatalf("Can't get configuration of database '%s' for task '%s': %s", dbName, taskName, err)
	}

	hostname := dbConfig.Host
	if dbConfig.Port > 0 {
		hostname = net.JoinHostPort(hostname, strconv.Itoa(dbConfig.Port))
	}

	uri := &url.URL{
		Scheme: dbConfig.Scheme,
		User:   url.UserPassword(dbConfig.Username, dbConfig.Password),
		Host:   hostname,
	}

	if dbConfig.Service != "" {
		uri.Path = url.PathEscape(dbConfig.Service)
	}

	params := dbConfig.Parameters
	if len(params) > 0 {
		query := uri.Query()
		for key, value := range params {
			query.Add(strings.TrimSpace(key), strings.TrimSpace(value))
		}
		uri.RawQuery = query.Encode()
	}

	return &DatabaseQuerySource{
		task:     taskName,
		database: dbName,
		driver:   dbConfig.Driver,
		uri:      uri.String(),
		query:    sourceConfig.Arguments["query"].(string),
	}
}

// `Run` creates a goroutine that reads data from the database and sends
// it to an output channel. It returns a channel that will receive the
// data read from the database.
func (src *DatabaseQuerySource) Run(wg *sync.WaitGroup) <-chan core.RowMap {
	log.Printf("* Creating DatabaseQuery source on database '%s'...", src.database)
	out := make(chan core.RowMap)

	wg.Add(1)
	go func() {
		defer wg.Done()

		log.Printf(" - Opening a connection to the database: '%s'...", src.database)
		db, err := sqlx.Open(src.driver, src.uri)
		if err != nil {
			log.Fatal("Error opening connection to database: ", err)
		}
		defer db.Close()

		log.Printf(" - Executing the database query: '%s'...", strings.TrimSpace(src.query[:24]))
		rows, err := db.Queryx(src.query)
		if err != nil {
			log.Fatal("Error trying to execute a query: ", err)
		}
		defer rows.Close()

		log.Printf(" - Fetching rows from the database: '%s'...", src.database)
		columns, _ := rows.Columns()
		length := len(columns)
		counter := 0
		for rows.Next() {
			row := make(core.RowMap, length)
			if err := rows.MapScan(row); err != nil {
				log.Fatal("Failed to scan map from current row: ", err)
			}

			counter++
			out <- row
		}

		close(out)
		log.Printf(" - Closing output channel after processed %d rows", counter)
	}()

	log.Printf("* DatabaseQuery source on database '%s' started successfully!", src.database)
	return out
}
