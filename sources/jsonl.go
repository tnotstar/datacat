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
	"bufio"
	"encoding/json"
	"log"
	"os"
	"sync"

	"github.com/tnotstar/sqltoapi/core"
)

// `JSONLFileSource` is the concrete implementation of the source interface
// for JSONLines (or NDJSON) file reader. It reads data from a given
// file(s) in NDJSON format and send each row to the output processing channel.
type JSONLFileSource struct {
	// The `task` of the task which is running into.
	task string
	// The `fileName` of the file to be read.
	fileName string
}

func IsaJSONLFileSource(sourceType string) bool {
	return sourceType == "jsonl-file-source"
}

// `NewJSONLFileSource` creates a new instance of the JSONLines source endpoint.
//
// The `task` is the name of the task to be executed.
// The `filename` is the name of the file to be read.
func NewJSONLFileSource(cfg core.Configurator, taskName string) *JSONLFileSource {
	sourceConfig, _ := cfg.GetSourceConfig(taskName)

	return &JSONLFileSource{
		task:     taskName,
		fileName: sourceConfig.Arguments["filename"].(string),
	}
}

// `Run` creates a goroutine that reads data from the database and sends
// it to an output channel. It returns a channel that will receive the
// data read from the database.
func (src *JSONLFileSource) Run(wg *sync.WaitGroup) <-chan core.RowMap {
	log.Printf("Starting JSONLines source for task %s...", src.task)
	out := make(chan core.RowMap)

	wg.Add(1)
	go func() {
		defer wg.Done()

		log.Printf("Reading input file: %s\n", src.fileName)
		reader, err := os.Open(src.fileName)
		if err != nil {
			log.Fatalf("Error opening file %s: %s", src.fileName, err.Error())
		}
		defer reader.Close()

		counter := 0
		scanner := bufio.NewScanner(reader)
		log.Println("Reading data from the input file")
		for scanner.Scan() {
			var row core.RowMap
			if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
				log.Fatalf("Error unmarshalling data row: %s", err.Error())
			}
			out <- row
			counter += 1
		}

		close(out)
		log.Printf("Read %d row(s) from the input file", counter)
	}()

	log.Println("JSONLines source for task:", src.task, ", started")
	return out
}
