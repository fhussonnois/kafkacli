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

type SchemaRegistryRestClient struct {
	host string
	port int
}

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

// Get a list of registered subjects.
func (client *SchemaRegistryRestClient) Subjects() []string {
	response := sendGetResponse("GET", client.subjectsEndPoint(), "")
	return unmarshalArrayString(response)
}

// Get a list of versions registered under the specified subject.
func (client *SchemaRegistryRestClient) Versions(subject string) []int {
	response := sendGetResponse("GET", client.subjectsEndPoint()+subject+"/versions", "")
	return unmarshalArrayInt(response)
}

// Get a specific version of the schema registered under this subject
func (client *SchemaRegistryRestClient) GetSubjectVersion(subject string, version string) string {
	response := sendGetResponse("GET", client.subjectsEndPoint()+subject+"/versions/"+version, "")
	return response
}

// Get global compatibility level.
func (client *SchemaRegistryRestClient) GetGlobalCompatibility() string {
	response := sendGetResponse("GET", client.hostname()+"/config", "")
	return response
}

// Get compatibility level for a subject.
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
