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
	"log"
	"os"
	"sync"

	"github.com/tnotstar/sqltoapi/core"
)

// `HttpTarget` is the concrete implementation of the target interface
// for HTTP microservices endpoints. It reads data from a given
// processing channel and send it to a given HTTP endpoint.
type HttpTarget struct {
	// The `task` of the task which is running into.
	task string
	// The `endpoint` to send data to.
	endpoint string
}

// `IsaHttpTarget` returns true if given target type
// is a HTTP.
func IsaHttpTarget(sourceType string) bool {
	return sourceType == "http-target"
}

// `NewJSONLinesTarget` creates a new instance of the JSONLines target endpoint.
//
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
func NewHttpTarget(cfg core.Configurator, taskName string) *HttpTarget {
	tgcfg := cfg.GetTargetConfig(taskName)
	return &HttpTarget{
		task:     taskName,
		endpoint: tgcfg.Output,
	}
}

// `Run` creates a goroutine that reads data from the database and sends
// it to an output channel. It returns a channel that will receive the
// data read from the database.
func (tgt *HttpTarget) Run(wg *sync.WaitGroup, in <-chan core.RowMap) {
	log.Printf("Starting HttpTarget target for task %s...", tgt.task)

	wg.Add(1)
	go func() {
		defer wg.Done()

		log.Printf("Creating output file: %s\n", tgt.endpoint)
		writer, err := os.Create(tgt.endpoint)
		if err != nil {
			log.Fatalf("Error creating file %s: %s", tgt.endpoint, err.Error())
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

		log.Printf("Wrote %d row(s) to the output file", counter)
	}()

	log.Printf("HttpTarget target for task %s started successfully", tgt.task)
}
