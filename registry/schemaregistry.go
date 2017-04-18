/*
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package registry

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
)

const (
	HTTP     = "HTTP://"
	SUBJECTS = "/subjects/"
)

// SchemaRegistryRestClient is a simple http-client to interact with a schema registry instance.
type SchemaRegistryRestClient struct {
	host string
	port int
}

// Create a new SchemaRegistryRestClient struct.
func NewRegistryClient(host string, port int) SchemaRegistryRestClient {
	return SchemaRegistryRestClient{
		host: host,
		port: port,
	}
}

func (client *SchemaRegistryRestClient) hostname() string {
	return HTTP + client.host + ":" + strconv.Itoa(client.port)
}

func (client *SchemaRegistryRestClient) subjectsEndPoint() string {
	return HTTP + client.host + ":" + strconv.Itoa(client.port) + SUBJECTS
}

// Subjects retrieves the list of registered subjects.
// Return subjects as an array of string.
func (client *SchemaRegistryRestClient) Subjects() []string {
	response := sendGetResponse("GET", client.subjectsEndPoint(), "")
	return unmarshalArrayString(response)
}

// Versions retrieves the list of versions registered under the specified subject.
// Return versions as an array of int.
func (client *SchemaRegistryRestClient) Versions(subject string) []int {
	response := sendGetResponse("GET", client.subjectsEndPoint()+subject+"/versions", "")
	return unmarshalArrayInt(response)
}

// GetSubjectVersion retrieves a specific version of the schema registered under this subject
// Return as JSON string.
func (client *SchemaRegistryRestClient) GetSubjectVersion(subject string, version string) string {
	response := sendGetResponse("GET", client.subjectsEndPoint()+subject+"/versions/"+version, "")
	return response
}

// GetGlobalCompatibility retrieves the global compatibility level.
// Return as JSON string.
func (client *SchemaRegistryRestClient) GetGlobalCompatibility() string {
	response := sendGetResponse("GET", client.hostname()+"/config", "")
	return response
}

// GetSubjectCompatibility retrieves the compatibility level for the specified subject.
// Return as JSON string.
func (client *SchemaRegistryRestClient) GetSubjectCompatibility(subject string) string {
	response := sendGetResponse("GET", client.hostname()+"/config/"+subject, "")
	return response
}

func unmarshalArrayString(s string) []string {
	res := make([]string, 0)
	err := json.Unmarshal([]byte(s), &res)
	if err != nil {
		panic(err)
	}
	return res
}

func unmarshalArrayInt(s string) []int {
	res := make([]int, 0)
	err := json.Unmarshal([]byte(s), &res)
	if err != nil {
		panic(err)
	}
	return res
}

func sendGetResponse(method string, url string, content string) string {
	req, err := http.NewRequest(method, url, bytes.NewBufferString(content))
	req.Header.Add("Content-Type", `application/json`)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	return string(body)
}

func send(method string, url string) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(nil))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
}
