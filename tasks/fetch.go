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

package tasks

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/tnotstar/sqltoapi/changers"
	"github.com/tnotstar/sqltoapi/config"
	"github.com/tnotstar/sqltoapi/core"
	"github.com/tnotstar/sqltoapi/sources"
)

// ExecuteFetch executes the fetch task with given name.
//
// The `taskName` is the name of the task to be executed.
func ExecuteFetch(taskName string) {
	log.Println("fetching...", taskName)

	cfg := config.Get()

	task, ok := cfg.Tasks.Fetch[taskName]
	if !ok {
		log.Fatal("missing configuration for task name: ", taskName)
	}

	dbName := task.Source.Database
	db, ok := cfg.Databases[dbName]
	if !ok {
		log.Fatal("invalid database name: ", dbName)
	}

	// checks if db.Driver is in a global list of valid drivers
	if db.Driver != "oracle" {
		log.Fatal("invalid driver for database: ", dbName)
	}

	//ch := make(chan map[string]any)
	dbDriver := db.Driver
	dbUri := db.URI
	dbQuery := task.Source.Query

	//go sources.SourceOracleTo(ch, dbUri, dbQuery)
	ch := sources.FromOracleQuery(dbDriver, dbUri, dbQuery)

	changes := task.Changes
	var ch2 <-chan core.RowMap

	for _, change := range changes {
		switch change.Type {
		case "cast-to-boolean":
			ch2 = changers.CastToBoolean(ch, change.Fields)
		default:
			log.Fatal("invalid change type: ", change.Type)
		}
	}

	for results := range ch2 {
		data, _ := json.Marshal(results)
		fmt.Printf("*> results: %v", string(data))
	}

	log.Println("all row fetched successfully")
}

// Add two numbers
func Add(x float64, y float64) float64 {
	return x + y
}
