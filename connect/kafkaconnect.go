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
package connect

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
)

// ConnectorStatus describes the configuration of a connector and its tasks.
type ConnectorTasksConfig struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
	Tasks  []struct {
		Connector string `json:"connector"`
		Task      int    `json:"task"`
	} `json:"tasks"`
}

// ConnectorStatus describes the states of a connector and its tasks.
type ConnectorStatus struct {
	Name      string `json:"name"`
	Connector struct {
		State    string `json:"state"`
		WorkerID string `json:"worker_id"`
	} `json:"connector"`
	Tasks []struct {
		State    string `json:"state"`
		ID       int    `json:"id"`
		WorkerID string `json:"worker_id"`
		Trace    string `json:"trace,omitempty"`
	} `json:"tasks"`
}

type ConnectorConfig struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
}

const (
	HTTP       = "HTTP://"
	CONNECTORS = "/connectors/"
)

// ConnectRestClient is a simple http-client to interact with a connector instances.
type ConnectRestClient struct {
	host string
	port int
}

// Create a new ConnectRestClient struct.
func NewConnectClient(host string, port int) ConnectRestClient {
	return ConnectRestClient{
		host: host,
		port: port,
	}
}

// Return a connector hostname.
func (client *ConnectRestClient) hostname() string {
	return HTTP + client.host + ":" + strconv.Itoa(client.port)
}

// Return the connect rest endpoint.
func (client *ConnectRestClient) connectEndPoint() string {
	return HTTP + client.host + ":" + strconv.Itoa(client.port) + CONNECTORS
}

// Getting a connect worker version.
// Return JSON string.
func (client *ConnectRestClient) Version() (r string, e error) {
	response, e := requestAndGetResponse("GET", client.hostname(), nil)
	if e == nil {
		r = string(response)
	}
	return
}

// Plugins lists all installed connectors plugins.
// Return the connector-plugins list as JSON string.
func (client *ConnectRestClient) Plugins() (r string, e error) {
	response, e := requestAndGetResponse("GET", client.hostname()+"/connector-plugins", nil)
	if e == nil {
		r = string(response)
	}
	return
}

// List lists all active connectors on a worker.
// Return the connector names as an array of string.
func (client *ConnectRestClient) List() (r []string, e error) {
	response, e := requestAndGetResponse("GET", client.connectEndPoint(), nil)
	if e == nil {
		err := json.Unmarshal(response, &r)
		if err != nil {
			panic(err)
		}
	}
	return
}

// Status gets status for a specified connector name.
// Return a new ConnectorStatus struct.
func (client *ConnectRestClient) Status(connector string) (r ConnectorStatus, e error) {
	response, e := requestAndGetResponse("GET", client.connectEndPoint()+connector+"/status", nil)
	if e == nil {
		err := json.Unmarshal(response, &r)
		if err != nil {
			panic(err)
		}
	}
	return
}

// Tasks describes tasks for the specified connector name.
// Return JSON string.
func (client *ConnectRestClient) Tasks(connector string) (r string, e error) {
	response, e := requestAndGetResponse("GET", client.connectEndPoint()+connector+"/tasks", nil)
	if e == nil {
		r = string(response)
	}
	return
}

// GetConfig retrieves the configuration for the specified connector.
// Return a new ConnectorConfig struct.
func (client *ConnectRestClient) GetConfig(connector string) (r ConnectorTasksConfig, e error) {
	response, e := requestAndGetResponse("GET", client.connectEndPoint()+connector, nil)
	if e == nil {
		err := json.Unmarshal(response, &r)
		if err != nil {
			panic(err)
		}
	}
	return
}

// Pause pauses all tasks for the specified connector name.
func (client *ConnectRestClient) Pause(connector string) error {
	_, e := requestAndGetResponse("PUT", client.connectEndPoint()+connector+"/pause", nil)
	return e
}

// Delete deletes all tasks for the specified connector name.
func (client *ConnectRestClient) Delete(connector string) error {
	_, e := requestAndGetResponse("DELETE", client.connectEndPoint()+connector, nil)
	return e
}

// Resume resumes all tasks for the specified connector name.
func (client *ConnectRestClient) Resume(connector string) error {
	_, e := requestAndGetResponse("PUT", client.connectEndPoint()+connector+"/resume", nil)
	return e
}

// Restart restarts the task identified by ID int for the specified connector.
func (client *ConnectRestClient) Restart(connector string, id int) error {
	fmt.Fprintf(os.Stdin, "Restarting task %d for connector %s \n", id, connector)
	_, e := requestAndGetResponse("POST", client.connectEndPoint()+connector+"/tasks/"+strconv.Itoa(id)+"/restart", nil)
	return e
}

// Create submit a new connector configuration.
// Return a JSON string describing the new connector configuration.
func (client *ConnectRestClient) Create(config ConnectorConfig) (r string, e error) {
	bytes, _ := json.Marshal(config)
	body := string(bytes)
	response, e := requestAndGetResponse("POST", client.connectEndPoint(), &body)
	if e == nil {
		r = string(response)
	}
	return
}

// Update modifies the configuration for the specified connector name.
// Return a JSON string describing the new connector configuration.
func (client *ConnectRestClient) Update(config ConnectorConfig) (r string, e error) {
	bytes, _ := json.Marshal(config.Config)
	body := string(bytes)
	response, e := requestAndGetResponse("PUT", client.connectEndPoint()+config.Name+"/config", &body)
	if e == nil {
		r = string(response)
	}
	return
}

func requestAndGetResponse(method string, url string, content * string) ([]byte, error) {
	var reqBody []byte = nil
	if (content != nil) {
		reqBody = []byte(*content)
	}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(reqBody))
	req.Header.Add("Content-Type", `application/json`)
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
