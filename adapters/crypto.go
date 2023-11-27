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
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/tnotstar/sqltoapi/core"
)

// `CryptoAESCBCZeroAdapter` apply a AES/CBC/ZeroPad block cipher
// transformation to the given fields.
type CryptoAESCBCZeroAdapter struct {
	// The `task` of the task which is running into.
	task string
	// The name of the `adapter`.
	adapter string
	// The `fields` to be casted.
	fields []string
	// The `direction` to be used to encrypt/decrypt.
	direction string
	// The `key` to be used to encrypt/decrypt as an array of bytes.
	key []byte
	// The `iv` to be used to encrypt/decrypt as an array of bytes.
	iv []byte
}

// `IsaCryptoAdapter` returns true if given adapter type is CryptoAESCBCZero.
func IsaCryptoAESCBCZeroAdapter(sourceType string) bool {
	return sourceType == "crypto-aescbczero-adapter"
}

// `NewCryptoAESCBCZeroAdapter` creates a new instance of the CryptoAESCBCZero adapter.
//
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
// The `adapterName` is the name of the adapter to be created.
func NewCryptoAESCBCZeroAdapter(cfg core.Configurator, taskName string, adapterName string) core.Adapter {
	adapterConfig, _ := cfg.GetAdapterConfig(taskName, adapterName)

	raws := adapterConfig.Arguments["fields"].([]any)
	fields := make([]string, len(raws))
	for i, field := range raws {
		fields[i] = fmt.Sprint(field)
	}

	direction := strings.ToLower(adapterConfig.Arguments["direction"].(string))
	if direction != "encrypt" && direction != "decrypt" {
		log.Fatalf("Invalid identifier for 'direction' parameter: %s", direction)
	}

	key, err := hex.DecodeString(adapterConfig.Arguments["key"].(string))
	if err != nil {
		log.Fatalf("Invalid hexadecimal string for 'key' parameter: %v", err)
	}

	iv, err := hex.DecodeString(adapterConfig.Arguments["iv"].(string))
	if err != nil {
		log.Fatalf("Invalid hexadecimal string for 'iv' parameter: %v", err)
	}

	return &CryptoAESCBCZeroAdapter{
		task:      taskName,
		adapter:   adapterName,
		fields:    fields,
		direction: direction,
		key:       key,
		iv:        iv,
	}
}

// CryptoAESCBCZero apply AES/CBC/Zeropad to given row fields values.
//
// Returns the output channel of the casted rows.
func (adp *CryptoAESCBCZeroAdapter) Run(wg *sync.WaitGroup, in <-chan core.RowMap) <-chan core.RowMap {
	log.Printf("* Creating CryptoAESCBCZero adapter for task %s...", adp.task)
	out := make(chan core.RowMap)

	wg.Add(1)
	go func() {
		defer wg.Done()

		log.Print(" - Reading from input channel of 'CryptoAESCBCZero' adapter...")
		counter := 0
		for row := range in {
			for _, field := range adp.fields {
				rawValue, ok := row[field].(string)
				if !ok {
					continue
				}
				if adp.direction == "decrypt" {
					plaintxt, err := decryptAESCBCZeropad(rawValue, adp.key, adp.iv)
					if err != nil {
						log.Fatalf("Error decrypting field '%s' of row %v: %s", field, row, err)
					}
					row[field] = plaintxt
				} else {
					ciphertxt, err := encryptAESCBCWithZeropad(rawValue, adp.key, adp.iv)
					if err != nil {
						log.Fatalf("Error encrypting field '%s' of row %v: %s", field, row, err)
					}
					row[field] = ciphertxt
				}
			}

			counter += 1
			out <- row
		}

		close(out)
		log.Printf(" - Closing output channel of CryptoAESCBCZero adapter (%d rows processed)", counter)
	}()

	log.Printf("* CryptoAESCBCZero adapter for task %s started successfully!", adp.task)
	return out
}

// Implements a AES/CBC/ZeroPad block cipher encryption algorithm.
//
// The `plaintxt` is the plain text to be encrypted.
// The `key` is the key to be used to encrypt as an array of bytes.
// The `iv` is the initialization vector to be used to encrypt as an array of bytes.
//
// Returns the cipher text as a base64 encoded string.
func encryptAESCBCWithZeropad(plaintxt string, key []byte, iv []byte) (string, error) {
	aes, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	encrypter := cipher.NewCBCEncrypter(aes, iv)
	blocksize := encrypter.BlockSize()

	bytestxt := []byte(plaintxt)
	byteslen := len(bytestxt)

	padding := blocksize - byteslen%blocksize
	if padding == blocksize {
		padding = 0
	}
	zeropad := bytes.Repeat([]byte{0}, padding)
	paddedtxt := append(bytestxt, zeropad...)

	ciphertxt := make([]byte, len(paddedtxt))
	encrypter.CryptBlocks(ciphertxt, paddedtxt)

	return base64.StdEncoding.EncodeToString(ciphertxt), nil
}

// Implements a AES/CBC/ZeroPad block cipher decryption algorithm.
//
// The `ciphertxt` is the cipher text to be decrypted.
// The `key` is the key to be used to decrypt as an array of bytes.
// The `iv` is the initialization vector to be used to decrypt as an array of bytes.
//
// Returns the plain text as a string.
func decryptAESCBCZeropad(ciphertxt string, key []byte, iv []byte) (string, error) {
	aes, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	decodedtxt, err := base64.StdEncoding.DecodeString(ciphertxt)
	if err != nil {
		return "", err
	}

	bytestxt := make([]byte, len(decodedtxt))
	decrypter := cipher.NewCBCDecrypter(aes, iv)
	decrypter.CryptBlocks(bytestxt, decodedtxt)

	zeropos := bytes.IndexByte(bytestxt, 0)
	if zeropos < 0 {
		zeropos = len(bytestxt)
	}

	return string(bytestxt[:zeropos]), nil
}
