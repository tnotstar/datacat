//
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

	_ "github.com/sijms/go-ora/v2"
)

// SourceOracleTo reads data from an Oracle database and sends it to the
// specified processing channel.
//
// The `dbUri` parameter is a string containing the database connection URI.
// The `dbQuery` parameter is a string containing the query to be executed.
//
func SourceOracleTo(out chan map[string]any, dbUri string, dbQuery string) {
	defer close(out)

	log.Println("starting oracle source...")
	db, err := sqlx.Open("oracle", dbUri)
	if err != nil {
		log.Fatal("error opening connection to database: ", err)
	}
	defer db.Close()

	log.Println("fetching data from oracle source...")
	rows, err := db.Queryx(dbQuery)
	if err != nil {
		log.Fatal("error executing query: ", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	length := len(columns)

	counter := 0
	for rows.Next() {
		results := make(map[string]any, length)
		if err := rows.MapScan(results); err != nil {
			log.Fatal("failed to scan row: ", err)
		}
		out <- results
		counter++
	}

    log.Printf("processed %d rows", counter)
}

