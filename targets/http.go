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
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"github.com/tnotstar/datacat/core"
)

// `HttpRequestTarget` is the concrete implementation of the target interface
// for HTTP microservices endpoints. It reads data from a given
// processing channel and send it to a given HTTP endpoint.
type HttpRequestTarget struct {
	// The `id` of the target.
	id int
	// The `task` of the task which is running into.
	task string
	// The `url` to send data to.
	url string
	// The `method` to use.
	method string
	// The `trustcert` is a flag to indicate if certificates must be trusted.
	trustcert bool
	// The `authzURL` is the authorization url to get the JWT token from.
	authzURL string
	// The `authzMethod` is the HTTP method to use for authorization.
	authzMethod string
	// The `authzClient` to use for authorization.
	authzClient string
	// The `authzCredential` to use for authorization.
	authzCredential string
}

// `IsaHttpRequestTarget` returns true if given target type
// is a HTTP.
func IsaHttpRequestTarget(sourceType string) bool {
	return sourceType == "http-request-target"
}

// `NewJSONLinesTarget` creates a new instance of the JSONLines target endpoint.
//
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
func NewHttpRequestTarget(id int, cfg core.Configurator, taskName string) *HttpRequestTarget {
	targetConfig, _ := cfg.GetTargetConfig(taskName)

	serviceName := targetConfig.Arguments["service"].(string)
	serviceConfig, err := cfg.GetServiceConfig(serviceName)
	if err != nil {
		log.Fatalf("Error getting configuration for service %s in task %s: %s", serviceName, taskName, err)
	}

	authzName := serviceConfig.WithAuthz
	authzConfig, err := cfg.GetServiceConfig(authzName)
	if err != nil {
		log.Fatalf("Error getting configuration for authz service %s in task %s: %s", authzName, taskName, err)
	}

	targetMethod := targetConfig.Arguments["method"].(string)
	targetPath := targetConfig.Arguments["path"].(string)
	targetURL, err := url.JoinPath(serviceConfig.BaseURL, targetPath)
	if err != nil {
		log.Fatalf("Error parsing endpoint URI: %s", err.Error())
	}

	return &HttpRequestTarget{
		id:              id,
		task:            taskName,
		url:             targetURL,
		method:          targetMethod,
		trustcert:       serviceConfig.TrustCert,
		authzURL:        authzConfig.BaseURL,
		authzMethod:     authzConfig.Method,
		authzClient:     authzConfig.Parameters["client"],
		authzCredential: authzConfig.Parameters["credential"],
	}
}

// `Run` creates a goroutine that reads data from the database and sends
// it to an output channel. It returns a channel that will receive the
// data read from the database.
func (tgt *HttpRequestTarget) Run(wg *sync.WaitGroup, in <-chan core.RowMap) {
	log.Printf("* Creating instance #%d of HTTP request target for task '%s'...", tgt.id, tgt.task)

	log.Println("Requesting JWToken from the authz server...")
	jwtoken := tgt.GetJWTokenFromAuthzServer()
	authorizationBearer := fmt.Sprintf("Bearer %s", jwtoken)
	log.Println("JWToken received successfully:", authorizationBearer)

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Transport: tr}

	wg.Add(1)
	go func() {
		defer wg.Done()

		counter := 0
		log.Println("Requesting service with received data rows...")
		for row := range in {
			buffer, err := json.Marshal(row)
			if err != nil {
				log.Fatalf("Error marshalling data row: %s", err.Error())
			}

			req, err := http.NewRequest("POST", tgt.url, bytes.NewBuffer(buffer))
			if err != nil {
				log.Fatalf("Error creating request: %s", err.Error())
			}
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("Authorization", authorizationBearer)

			reqDump, err := httputil.DumpRequestOut(req, true)
			if err == nil {
				log.Print("Requesting:\n", string(reqDump))
			} else {
				log.Print("Error dumping request:", err.Error())
			}

			res, err := client.Do(req)
			if err != nil {
				log.Fatalf("Error sending request: %s", err.Error())
			}
			res.Body.Close()

			log.Println("Sending data row:", res.Status)

			counter += 1
		}

		log.Printf("Requested %d row(s) to the target service", counter)
	}()

	log.Printf("HttpTarget target for task %s started successfully", tgt.task)
}

// `GetJWTokenFromAuthzServer` request and return a JWToken for the target endpoint.
func (tgt *HttpRequestTarget) GetJWTokenFromAuthzServer() string {
	url, err := url.Parse(tgt.authzURL)
	if err != nil {
		log.Fatalf("Error parsing authz URI: %s", err.Error())
	}

	query := url.Query()
	query.Set("client", tgt.authzClient)
	query.Set("credential", tgt.authzCredential)
	url.RawQuery = query.Encode()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		log.Fatalf("Error creating request for authz: %s", err.Error())
	}

	log.Printf("Requesting authz token from: %s", tgt.authzURL)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: tgt.trustcert},
	}

	client := &http.Client{Transport: tr}
	res, err := client.Do(req)
	if err != nil {
		log.Fatalf("Error requesting authz: %s", err.Error())
	}
	defer res.Body.Close()

	var jsonBody any
	if err := json.NewDecoder(res.Body).Decode(&jsonBody); err != nil {
		log.Fatalf("Error decoding authz response: %s", err.Error())
	}

	rawToken := jsonBody.(map[string]any)["token"]
	return fmt.Sprint(rawToken)
}
