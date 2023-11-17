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
	"fmt"
	"log"
	"net/url"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/tnotstar/sqltoapi/core"

	_ "github.com/sijms/go-ora/v2"
)

// `OracleSource` is the concrete implementation of the source interface
// for Oracle databases. It reads data from an Oracle database and sends
// it to the output processing channel.
type OracleSource struct {
	// `task` is the name of the task which is running into.
	task string
	// `driver` is the name of the database driver to be used.
	driver string
	// `uri` is a string containing the connection URI.
	uri string
	// `query` is a string containing the query to be executed.
	query string
}

// `IsaOracleSource` returns true if given source type is
// an Oracle Query.
func IsaOracleSource(sourceType string) bool {
	return sourceType == "oracle-query"
}

// `NewOracleSource` creates a new instance of the Oracle Source endpoint.
//
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
func NewOracleSource(cfg core.Configurator, taskName string) core.Source {
	srcfg := cfg.GetSourceConfig(taskName)
	dbcfg := cfg.GetDatabaseConfig(srcfg.Database)

	uri := &url.URL{
		Scheme: "oracle",
		User:   url.UserPassword(dbcfg.Username, dbcfg.Password),
		Host:   fmt.Sprintf("%s:%d", dbcfg.Host, dbcfg.Port),
		Path:   dbcfg.Service,
	}

	return &OracleSource{
		task:   taskName,
		driver: "oracle",
		uri:    uri.String(),
		query:  srcfg.Query,
	}
}

// `Run` creates a goroutine that reads data from the database and sends
// it to an output channel. It returns a channel that will receive the
// data read from the database.
func (src *OracleSource) Run(wg *sync.WaitGroup) <-chan core.RowMap {
	out := make(chan core.RowMap)

	wg.Add(1)
	go func() {
		defer wg.Done()

		log.Printf("Opening a connection to the database for task: %s", src.task)
		db, err := sqlx.Open(src.driver, src.uri)
		if err != nil {
			log.Fatal("Error opening connection to database: ", err)
		}
		defer db.Close()

		log.Printf("Executing the database query: %s...", src.query[:24])
		rows, err := db.Queryx(src.query)
		if err != nil {
			log.Fatal("Error trying to execute a query: ", err)
		}
		defer rows.Close()

		log.Println("Fetching rows from the database")
		columns, _ := rows.Columns()
		length := len(columns)
		counter := 0
		for rows.Next() {
			results := make(core.RowMap, length)
			if err := rows.MapScan(results); err != nil {
				log.Fatal("Failed to scan map from current row: ", err)
			}
			counter++
			out <- results
		}

		close(out)
		log.Printf("Closing output channel after processed %d rows", counter)
	}()

	return out
}
