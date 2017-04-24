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
	"errors"
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

type ID struct {
	Value int `json:"id"`
}

type NewSchemaVersion struct {
	Subject string `json:"subject"`
	ID      int    `json:"id"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

type Compatibility struct {
	Value string `json:"compatibility"`
}

type CompatibilityLevel struct {
	Value string `json:"compatibilityLevel"`
}

type IsCompatible struct {
	Value bool `json:"is_compatible"`
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
func (client *SchemaRegistryRestClient) Subjects() (r []string, e error) {
	response, e := sendGetResponse("GET", client.subjectsEndPoint(), "")
	if e == nil {
		r = unmarshalArrayString(string(response))
	}
	return
}

// Versions retrieves the list of versions registered under the specified subject.
// Return versions as an array of int.
func (client *SchemaRegistryRestClient) Versions(subject string) (r []int, e error) {
	response, e := sendGetResponse("GET", client.subjectsEndPoint()+subject+"/versions", "")
	if e == nil {
		r = unmarshalArrayInt(string(response))
	}
	return
}

// GetSubjectVersion retrieves a specific version of the schema registered under this subject.
// Return a new SchemaVersion struct.
func (client *SchemaRegistryRestClient) GetSubjectVersion(subject string, version string) (r SchemaVersion, e error) {
	response, e := sendGetResponse("GET", client.subjectsEndPoint()+subject+"/versions/"+version, "")
	if e == nil {
		err := json.Unmarshal(response, &r)
		if err != nil {
			panic(err)
		}
	}
	return
}

// Register registers a new schema under the specified subject.
// Return a new NewSchemaVersion struct.
func (client *SchemaRegistryRestClient) Register(subject string, schema Schema) (r ID, e error) {
	body, _ := json.Marshal(schema)
	response, e := sendGetResponse("POST", client.subjectsEndPoint()+subject+"/versions", string(body))
	if e == nil {
		err := json.Unmarshal(response, &r)
		if err != nil {
			panic(err)
		}
	}
	return
}

// Exists checks if a schema has already been registered under the specified subject.
// If so, this returns the schema string along with its globally unique identifier, its version under this subject and the subject name.
// Return a new NewSchemaVersion struct.
func (client *SchemaRegistryRestClient) Exists(subject string, schema Schema) (r NewSchemaVersion, e error) {
	body, _ := json.Marshal(schema)
	response, e := sendGetResponse("POST", client.subjectsEndPoint()+subject, string(body))
	if e == nil {
		err := json.Unmarshal(response, &r)
		if err != nil {
			panic(err)
		}
	}
	return
}

// GetGlobalCompatibility retrieves the global compatibility level.
// Return as JSON string.
func (client *SchemaRegistryRestClient) GetGlobalCompatibility() (r string, e error) {
	response, e := sendGetResponse("GET", client.hostname()+"/config", "")
	r = string(response)
	return
}

// GetSubjectCompatibility retrieves the compatibility level for the specified subject.
// Return as JSON string.
func (client *SchemaRegistryRestClient) GetSubjectCompatibility(subject string) (r CompatibilityLevel, e error) {
	response, e := sendGetResponse("GET", client.hostname()+"/config/"+subject, "")
	if e == nil {
		err := json.Unmarshal(response, &r)
		if err != nil {
			panic(err)
		}
	}
	return
}

// UpdateSubjectCompatibility sets the compatibility level for the specified subject.
// Return the new Compatibility.
func (client *SchemaRegistryRestClient) UpdateSubjectCompatibility(subject string, compatibility Compatibility) (r Compatibility, e error) {
	body, _ := json.Marshal(compatibility)
	response, e := sendGetResponse("PUT", client.hostname()+"/config/"+subject, string(body))
	if e == nil {
		err := json.Unmarshal(response, &r)
		if err != nil {
			panic(err)
		}
	}
	return
}

// UpdateSubjectCompatibility tests schemas for compatibility against specific versions of a subject’s schema.
// Return the new Compatibility.
func (client *SchemaRegistryRestClient) CheckSubjectCompatibility(subject string, versionId string, schema Schema) (r IsCompatible, e error) {
	body, _ := json.Marshal(schema)
	response, e := sendGetResponse("POST", client.hostname()+"/compatibility/subjects/"+subject+"/versions/"+versionId, string(body))
	if e == nil {
		err := json.Unmarshal(response, &r)
		if err != nil {
			panic(err)
		}
	}
	return
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

func sendGetResponse(method string, url string, content string) ([]byte, error) {
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

	if resp.StatusCode >= 400 && resp.StatusCode <= 500 {
		return nil, errors.New(string(body))
	}
	return body, nil
}
