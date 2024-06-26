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

package targets

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/tnotstar/datacat/core"
)

// `JSONLinesTarget` is the concrete implementation of the target interface
// for JSONLines (or NDJSON) file writer. It reads data from a given
// processing channel and write it to a file in NDJSON format.
type JSONLinesTarget struct {
	// The `id` of the target.
	id int
	// The `task` of the task which is running into.
	task string
	// The `fileName` of the file to be created.
	fileName string
	// The `batchSize` of the batch to be written.
	batchSize int
}

// `IsaJSONLFileTarget` returns true if given target type
// is a JSONLines.
func IsaJSONLFileTarget(sourceType string) bool {
	return sourceType == "jsonl-file-target"
}

// `NewJSONLFileTarget` creates a new instance of the JSONLines target endpoint.
//
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
func NewJSONLFileTarget(id int, cfg core.Configurator, taskName string) *JSONLinesTarget {
	targetConfig, _ := cfg.GetTargetConfig(taskName)

	fileName := fmt.Sprint(targetConfig.Arguments["filename"])
	batchSize, _ := strconv.Atoi(fmt.Sprint(targetConfig.Arguments["batchsize"]))

	return &JSONLinesTarget{
		id:        id,
		task:      taskName,
		fileName:  fileName,
		batchSize: batchSize,
	}
}

// `Run` creates a goroutine that reads data from the database and sends
// it to an output channel. It returns a channel that will receive the
// data read from the database.
func (tgt *JSONLinesTarget) Run(wg *sync.WaitGroup, in <-chan core.RowMap) {
	log.Printf("* Creating instance #%d of JSONLines file target for task '%s'...", tgt.id, tgt.task)

	wg.Add(1)
	go func() {
		defer wg.Done()

		fileName := fmt.Sprintf(tgt.fileName, tgt.id)
		log.Printf(" - Creating JSONLines target file: '%s'...", fileName)

		writer, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("Error creating file %s: %s", fileName, err.Error())
		}
		defer writer.Close()

		counter := 0
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

			counter++
		}

		log.Printf(" - Written %d row(s) to the JSONLines target file: '%s'...", counter, fileName)
	}()

	log.Printf("* JSONLines target with filename pattern '%s' started successfully!", tgt.fileName)
}
