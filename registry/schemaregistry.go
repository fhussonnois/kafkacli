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

const (
	HEADER_ACCEPT       = `application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json`
	HEADER_CONTENT_TYPE = `application/json`
)

type SchemaVersion struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

type NewSchemaVersion struct {
	Subject string `json:"subject"`
	ID      string `json:"id"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

type Compatibility struct {
	Value string `json:"compatibility"`
}

type Schema struct {
	Value string `json:"schema"`
}

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
	return client.hostname() + SUBJECTS
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

// GetSubjectVersion retrieves a specific version of the schema registered under this subject.
// Return a new SchemaVersion struct.
func (client *SchemaRegistryRestClient) GetSubjectVersion(subject string, version string) SchemaVersion {
	response := sendGetResponse("GET", client.subjectsEndPoint()+subject+"/versions/"+version, "")
	var ret SchemaVersion
	err := json.Unmarshal([]byte(response), &ret)
	if err != nil {
		panic(err)
	}
	return ret
}

// Register registers a new schema under the specified subject.
// Return a new NewSchemaVersion struct.
func (client *SchemaRegistryRestClient) Register(subject string, schema Schema) NewSchemaVersion {
	body, _ := json.Marshal(schema)
	response := sendGetResponse("POST", client.subjectsEndPoint()+subject, string(body))
	var ret NewSchemaVersion
	err := json.Unmarshal([]byte(response), &ret)
	if err != nil {
		panic(err)
	}
	return ret
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

// UpdateSubjectCompatibility sets the compatibility level for the specified subject.
// Return the new Compatibility.
func (client *SchemaRegistryRestClient) UpdateSubjectCompatibility(subject string, compatibility Compatibility) Compatibility {
	body, _ := json.Marshal(compatibility)
	response := sendGetResponse("PUT", client.hostname()+"/config/"+subject, string(body))
	var ret Compatibility
	err := json.Unmarshal([]byte(response), &ret)
	if err != nil {
		panic(err)
	}
	return ret
}

// UpdateSubjectCompatibility tests schemas for compatibility against specific versions of a subject’s schema.
// Return the new Compatibility.
func (client *SchemaRegistryRestClient) CheckSubjectCompatibility(subject string, versionId string, schema Schema) Compatibility {
	body, _ := json.Marshal(schema)
	response := sendGetResponse("POST", client.hostname()+"/compatibility/subjects/"+subject+"/versions/"+versionId, string(body))
	var ret Compatibility
	err := json.Unmarshal([]byte(response), &ret)
	if err != nil {
		panic(err)
	}
	return ret
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
	req.Header.Add("Content-Type", HEADER_CONTENT_TYPE)
	req.Header.Add("Accept", HEADER_ACCEPT)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	return string(body)
}
