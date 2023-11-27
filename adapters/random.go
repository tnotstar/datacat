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

package adapters

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tnotstar/sqltoapi/core"
)

// `NamesRandomizerAdapter` an adapter to randomize names.
type NamesRandomizerAdapter struct {
	// The `task` of the task which is running into.
	task string
	// The name of the `adapter`.
	adapter string
	// The `firstname` is the field for first names generation.
	firstName string
	// The `lastname` is the field for last names generation.
	lastName string
	// The `maleFlag` is the field for male/femal selection.
	maleFlag string
	// The `rg` is the random generator.
	rng *rand.Rand
	// The `allData` are statistics for last names.
	allData []nameData
	// The `maleData` are statistics for male given names.
	maleData []nameData
	// The `femaleData` are statistics for female given names.
	femaleData []nameData
}

// `IsaNamesRandomizerAdapter` returns true if given adapter type
// is NamesRandomizerAdapter.
func IsaNamesRandomizerAdapter(adapterType string) bool {
	return adapterType == "names-randomizer-adapter"
}

// `NewNamesRandomizerAdapter` creates a new instance of the NamesRandomizer adapter.
//
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
// The `adapterName` is the name of the adapter to be created.
func NewNamesRandomizerAdapter(cfg core.Configurator, taskName string, adapterName string) core.Adapter {
	adapterConfig, _ := cfg.GetAdapterConfig(taskName, adapterName)

	firstName := fmt.Sprint(adapterConfig.Arguments["firstname"])
	lastName := fmt.Sprint(adapterConfig.Arguments["lastname"])
	maleFlag := fmt.Sprint(adapterConfig.Arguments["maleflag"])

	randomArgs, ok := adapterConfig.Arguments["random"].(map[string]any)
	if !ok {
		log.Fatalf("Invalid random arguments: %v", randomArgs)
	}

	basePath := filepath.Dir(cfg.GetConfigFilename())
	allFilename := core.ResolveFilename(basePath, fmt.Sprint(randomArgs["lastnames"]))
	maleFilename := core.ResolveFilename(basePath, fmt.Sprint(randomArgs["malenames"]))
	femaleFilename := core.ResolveFilename(basePath, fmt.Sprint(randomArgs["femalenames"]))

	rng := newRandomGenerator(fmt.Sprint(randomArgs["seed"]))
	allData := getNamesData(allFilename)
	maleData := getNamesData(maleFilename)
	femaleData := getNamesData(femaleFilename)

	return &NamesRandomizerAdapter{
		task:       taskName,
		adapter:    adapterName,
		firstName:  firstName,
		lastName:   lastName,
		maleFlag:   maleFlag,
		rng:        rng,
		allData:    allData,
		maleData:   maleData,
		femaleData: femaleData,
	}
}

// NamesRandomizer randomize names values.
//
// Returns the output channel of the casted rows.
func (adp *NamesRandomizerAdapter) Run(wg *sync.WaitGroup, in <-chan core.RowMap) <-chan core.RowMap {
	out := make(chan core.RowMap)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for row := range in {
			if _, ok := row[adp.firstName]; ok {
				maleFlag := strings.ToUpper(strings.Trim(fmt.Sprint(row[adp.maleFlag]), " "))
				if strings.HasPrefix(maleFlag, "M") {
					row[adp.firstName] = getRandomName(&adp.maleData)
				} else {
					row[adp.firstName] = getRandomName(&adp.femaleData)
				}
			}

			if _, ok := row[adp.lastName]; ok {
				row[adp.lastName] = getRandomName(&adp.allData)
			}

			out <- row
		}

		close(out)
	}()

	return out
}

// The `nameData` is the usage statistics of the name.
type nameData struct {
	name    string
	cumFreq float64
}

// `newRandomGenerator` creates a new random generator with given seed.
func newRandomGenerator(raw string) *rand.Rand {
	seed, err := strconv.ParseInt(raw, 10, 64)
	if err == nil {
		seed = time.Now().UnixNano()
	}
	return rand.New(rand.NewSource(seed))
}

// `getNamesData` returns the names data from a file with given filename.
func getNamesData(filename string) []nameData {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error opening file %s: %v", filename, err)
	}
	defer file.Close()

	var names []nameData = make([]nameData, 0)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}

		name := fields[0]
		cumFreq, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}

		names = append(names, nameData{
			name:    name,
			cumFreq: cumFreq,
		})
	}

	return names
}

// `getRandomName` returns a random name from the given data.
func getRandomName(data *[]nameData) string {
	count := len(*data)
	max := (*data)[count-1].cumFreq
	rn := rand.Float64() * max
	idx := sort.Search(count, func(i int) bool {
		return (*data)[i].cumFreq >= rn
	})
	return (*data)[idx].name
}
