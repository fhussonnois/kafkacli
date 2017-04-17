/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Simple Command line interface (CLI) to manage connectors though the Kafka Connect REST Interface.
 *
 **/
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

type ConnectorConfig struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
	Tasks  []struct {
		Connector string `json:"connector"`
		Task      int    `json:"task"`
	} `json:"tasks"`
}

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
	} `json:"tasks"`
}

const (
	HTTP       = "HTTP://"
	CONNECTORS = "/connectors/"
)

type ConnectRestClient struct {
	host string
	port int
}

func NewConnectClient(host string, port int) ConnectRestClient {
	return ConnectRestClient{
		host: host,
		port: port,
	}
}

// Getting a connect worker version.
func (client *ConnectRestClient) hostname() string {
	return HTTP + client.host + ":" + strconv.Itoa(client.port)
}

// Getting a connect worker version.
func (client *ConnectRestClient) connectEndPoint() string {
	return HTTP + client.host + ":" + strconv.Itoa(client.port) + CONNECTORS
}

// Getting a connect worker version.
func (client *ConnectRestClient) Version() string {
	return sendGetResponse("GET", client.hostname(), "")
}

// Listing installed connectors plugins.
func (client *ConnectRestClient) Plugins() string {
	return sendGetResponse("GET", client.hostname()+"/connector-plugins", "")
}

// Listing active connectors on a worker.
func (client *ConnectRestClient) List() []string {
	response := sendGetResponse("GET", client.connectEndPoint(), "")
	connectors := make([]string, 0)
	err := json.Unmarshal([]byte(response), &connectors)
	if err != nil {
		panic(err)
	}
	return connectors
}

// Getting connector status.
func (client *ConnectRestClient) Status(connector string) ConnectorStatus {
	response := sendGetResponse("GET", client.connectEndPoint()+connector+"/status", "")
	var connectStatus ConnectorStatus
	err := json.Unmarshal([]byte(response), &connectStatus)
	if err != nil {
		panic(err)
	}
	return connectStatus
}

// Getting tasks for a connector.
func (client *ConnectRestClient) Tasks(connector string) string {
	return sendGetResponse("GET", client.connectEndPoint()+connector+"/tasks", "")
}

// Getting connector configuration.
func (client *ConnectRestClient) GetConfig(connector string) ConnectorConfig {
	response := sendGetResponse("GET", client.connectEndPoint()+connector, "")
	var config ConnectorConfig
	err := json.Unmarshal([]byte(response), &config)
	if err != nil {
		panic(err)
	}
	return config
}

// Pausing a connector.
func (client *ConnectRestClient) Pause(connector string) {
	fmt.Fprintf(os.Stdin, "Pausing connector %s \n", connector)
	send("PUT", client.connectEndPoint()+connector+"/pause")
}

// Deleting a connector.
func (client *ConnectRestClient) Delete(connector string) {
	fmt.Fprintf(os.Stdin, "Deleting connector %s \n", connector)
	send("DELETE", client.connectEndPoint()+connector)
}

// Resuming a connector.
func (client *ConnectRestClient) Resume(connector string) {
	fmt.Fprintf(os.Stdin, "Resuming connector %s \n", connector)
	send("PUT", client.connectEndPoint()+connector+"/resume")
}

// Restarting tasks.
func (client *ConnectRestClient) Restart(connector string, task int) {
	fmt.Fprintf(os.Stdin, "Restarting task %d for connector %s \n", task, connector)
	send("POST", client.connectEndPoint()+connector+"/tasks/"+strconv.Itoa(task)+"/restart")
}

// Creating a new connector.
func (client *ConnectRestClient) Create(config string) string {
	return sendGetResponse("POST", client.connectEndPoint(), config)
}

// Updating a connector configuration.
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
