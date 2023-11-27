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
	"log"

	"github.com/tnotstar/sqltoapi/core"
)

// `BuildAdapter` creates a new instance of the adapter middlepoint specified
// by the configuration object passed as argument.
//
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
// The `adapterName` is the index of the adapter to be created.
func BuildAdapter(cfg core.Configurator, taskName string, adapterName string) core.Adapter {
	adapterConfig, err := cfg.GetAdapterConfig(taskName, adapterName)
	if err != nil {
		log.Fatalf("Error getting adapters configuration for task %s: %s", taskName, err)
	}

	if IsaCastToDatatypeAdapter(adapterConfig.Type) {
		return NewCastToDatatypeAdapter(cfg, taskName, adapterName)
	}

	if IsaCryptoAESCBCZeroAdapter(adapterConfig.Type) {
		return NewCryptoAESCBCZeroAdapter(cfg, taskName, adapterName)
	}

	if IsaNullHandlingAdapter(adapterConfig.Type) {
		return NewNullHandlingAdapter(cfg, taskName, adapterName)
	}

	if IsaNamesRandomizerAdapter(adapterConfig.Type) {
		return NewNamesRandomizerAdapter(cfg, taskName, adapterName)
	}

	log.Fatalf("Invalid adapter middlepoint type %s", adapterConfig.Type)
	return nil
}
