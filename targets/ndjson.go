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

package targets

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	"github.com/tnotstar/sqltoapi/core"
)

// `JSONLinesTarget` is the concrete implementation of the target interface
// for JSONLines (or NDJSON) file generator. It reads data from a given
// processing channel and write it to a file in NDJSON format.
type JSONLinesTarget struct {
	// The `wg` of the wait group to be used to sync all targets.
	wg *sync.WaitGroup
	// The `task` of the task which is running into.
	task string
	// The `fileName` of the file to be created.
	fileName string
}

// `NewJSONLinesTarget` creates a new instance of the JSONLines endpoint.
//
// The `wg` is a pointer to the wait group to be used to sync all targets.
// The `task` is the name of the task to be executed.
// The `filename` is the name of the file to be created.
func NewJSONLinesTarget(wg *sync.WaitGroup, task string, fileName string) *JSONLinesTarget {
	return &JSONLinesTarget{
		wg:       wg,
		task:     task,
		fileName: fileName,
	}
}

// `Run` creates a goroutine that reads data from the database and sends
// it to an output channel. It returns a channel that will receive the
// data read from the database.
func (tgt *JSONLinesTarget) Run(in <-chan core.RowMap) {
	log.Println("Starting JSONLines target")
	go func() {
		log.Printf("Creating output file: %s\n", tgt.fileName)
		writer, err := os.Create(tgt.fileName)
		if err != nil {
			log.Fatalf("Error creating file %s: %s", tgt.fileName, err.Error())
		}
		defer writer.Close()

		counter := 0
		log.Println("Writing data to the output file")
		for row := range in {
			buffer, err := json.Marshal(row)
			if err != nil {
				log.Fatalf("Error marshalling data row: %s", err.Error())
			}
			if _, err := writer.Write(buffer); err != nil {
				log.Fatalf("Error writing data row: %s", err.Error())
			}
			if _, err := writer.WriteString("\n"); err != nil {
				log.Fatalf("Error writing line terminator: %s", err.Error())
			}
			counter += 1
		}

		tgt.wg.Done()
		log.Printf("Wrote %d row(s) to the output file\n", counter)
	}()
}
