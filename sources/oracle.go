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
//

package sources

import (
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/tnotstar/sqltoapi/core"

	_ "github.com/sijms/go-ora/v2"
)

// FromOracleQuery reads data from an Oracle database and sends it to the
// specified processing channel.
//
// The `driver` parameter is a string cotaining the name of the database driver.
// The `uri` parameter is a string containing the database connection URI.
// The `query` parameter is a string containing the query to be executed.
func FromOracleQuery(driver string, uri string, query string) <-chan core.RowMap {
	out := make(chan core.RowMap)

	go func() {
		log.Println("Opening a connection to the database")
		db, err := sqlx.Open(driver, uri)
		if err != nil {
			log.Fatal("Error opening connection to database: ", err)
		}
		defer db.Close()

		log.Println("Executing the database query")
		rows, err := db.Queryx(query)
		if err != nil {
			log.Fatal("Error executing query: ", err)
		}
		defer rows.Close()

		log.Println("Fetch rows from the database")
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
		log.Printf("Processed %d rows", counter)
	}()

	return out
}
