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
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
)

// ConnectorStatus describes the configuration of a connector and its tasks.
type ConnectorConfig struct {
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

type Config struct {
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
func (client *ConnectRestClient) Version() string {
	return sendGetResponse("GET", client.hostname(), "")
}

// Plugins lists all installed connectors plugins.
// Return the connector-plugins list as JSON string.
func (client *ConnectRestClient) Plugins() string {
	return sendGetResponse("GET", client.hostname()+"/connector-plugins", "")
}

// List lists all active connectors on a worker.
// Return the connector names as an array of string.
func (client *ConnectRestClient) List() []string {
	response := sendGetResponse("GET", client.connectEndPoint(), "")
	connectors := make([]string, 0)
	err := json.Unmarshal([]byte(response), &connectors)
	if err != nil {
		panic(err)
	}
	return connectors
}

// Status gets status for a specified connector name.
// Return a new ConnectorStatus struct.
func (client *ConnectRestClient) Status(connector string) ConnectorStatus {
	response := sendGetResponse("GET", client.connectEndPoint()+connector+"/status", "")
	var connectStatus ConnectorStatus
	err := json.Unmarshal([]byte(response), &connectStatus)
	if err != nil {
		panic(err)
	}
	return connectStatus
}

// Tasks describes tasks for the specified connector name.
// Return JSON string.
func (client *ConnectRestClient) Tasks(connector string) string {
	return sendGetResponse("GET", client.connectEndPoint()+connector+"/tasks", "")
}

// GetConfig retrieves the configuration for the specified connector.
// Return a new ConnectorConfig struct.
func (client *ConnectRestClient) GetConfig(connector string) ConnectorConfig {
	response := sendGetResponse("GET", client.connectEndPoint()+connector, "")
	var config ConnectorConfig
	err := json.Unmarshal([]byte(response), &config)
	if err != nil {
		panic(err)
	}
	return config
}

// Pause pauses all tasks for the specified connector name.
func (client *ConnectRestClient) Pause(connector string) {
	fmt.Fprintf(os.Stdin, "Pausing connector %s \n", connector)
	send("PUT", client.connectEndPoint()+connector+"/pause")
}

// Delete deletes all tasks for the specified connector name.
func (client *ConnectRestClient) Delete(connector string) {
	fmt.Fprintf(os.Stdin, "Deleting connector %s \n", connector)
	send("DELETE", client.connectEndPoint()+connector)
}

// Resume resumes all tasks for the specified connector name.
func (client *ConnectRestClient) Resume(connector string) {
	fmt.Fprintf(os.Stdin, "Resuming connector %s \n", connector)
	send("PUT", client.connectEndPoint()+connector+"/resume")
}

// Restart restarts the task identified by ID int for the specified connector.
func (client *ConnectRestClient) Restart(connector string, id int) {
	fmt.Fprintf(os.Stdin, "Restarting task %d for connector %s \n", id, connector)
	send("POST", client.connectEndPoint()+connector+"/tasks/"+strconv.Itoa(id)+"/restart")
}

// Create submit a new connector configuration.
// Return a JSON string describing the new connector configuration.
func (client *ConnectRestClient) Create(config Config) string {
	body, _ := json.Marshal(config)
	return sendGetResponse("POST", client.connectEndPoint(), string(body))
}

// Update modifies the configuration for the specified connector name.
// Return a JSON string describing the new connector configuration.
func (client *ConnectRestClient) Update(connector string, config string) string {
	return sendGetResponse("PUT", client.connectEndPoint()+connector+"/config", config)
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
