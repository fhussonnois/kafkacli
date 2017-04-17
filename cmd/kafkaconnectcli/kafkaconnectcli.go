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
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/fhussonnois/kafkacli/connect"
	"os"
	"regexp"
	"strings"
)

type Matcher func(string) bool

func findMatchingConnectors(client connect.ConnectRestClient, fn Matcher) []string {
	var matchConnectors []string

	connectorsString := client.List()
	activeConnectors := make([]string, 0)
	err := json.Unmarshal([]byte(connectorsString), &activeConnectors)
	if err != nil {
		panic(err)
	}
	for _, conn := range activeConnectors {
		if fn(conn) {
			matchConnectors = append(matchConnectors, conn)
		}
	}
	return matchConnectors
}

func getUnmarshalConnectStatus(client connect.ConnectRestClient, connector string) connect.ConnectorStatus {
	jsonStatus := client.Status(connector)
	var connectStatus connect.ConnectorStatus
	err := json.Unmarshal([]byte(jsonStatus), &connectStatus)
	if err != nil {
		panic(err)
	}
	return connectStatus
}

func usage() {
	fmt.Println("A simple Command line interface (CLI) to manage connectors through the Kafka Connect REST Interface.\n")
	fmt.Fprintf(os.Stderr, "Usage of %s: command [arguments] \n", os.Args[0])
	fmt.Println("The commands are : \n")
	fmt.Println("	list		Listing active connectors on a worker.")
	fmt.Println("	config		Getting connector configuration.")
	fmt.Println("	create		Creating a new connector.")
	fmt.Println("	delete		Deleting a connector.")
	fmt.Println("	delete-all	Deleting all connectors.")
	fmt.Println("	pause		Pausing a connector (useful if downtime is needed for the system the connector interacts with).")
	fmt.Println("	plugins		Listing installed connectors plugins.")
	fmt.Println("	resume		Restarting a connector.")
	fmt.Println("	restart-failed	Restarting failed tasks for a connector.")
	fmt.Println("	status		Getting connector status.")
	fmt.Println("	tasks		Getting tasks for a connector.")
	fmt.Println("	scale		Scaling up/down the number of tasks for a connector.")
	fmt.Println("	update		Updating connector configuration.")
	fmt.Println("	version		Getting a connect worker version.")
	fmt.Println("\nUse \"kafkaconnectcli help [command]\" for more information about that command.")
	os.Exit(1)
}

func main() {

	commonCommand := flag.NewFlagSet("Worker", flag.ExitOnError)
	commonPort := commonCommand.Int("port", 8083, "The connector worker port. (Required)")
	commonHost := commonCommand.String("host", "localhost", "The connector worker host address. (Required)")

	connectorCommand := flag.NewFlagSet("Connector", flag.ExitOnError)
	connectName := connectorCommand.String("connector", "", "The connector name or a regex. (Required)")
	connectPort := connectorCommand.Int("port", 8083, "The connector worker port.")
	connectHost := connectorCommand.String("host", "localhost", "The connector worker host address.")

	listCommand := flag.NewFlagSet("List", flag.ExitOnError)
	listState := listCommand.String("with-state", "", "Filter on connector/task for the specified state [running|failed|paused|unassigned]")
	listPort := listCommand.Int("port", 8083, "The connector worker port. (Required)")
	listHost := listCommand.String("host", "localhost", "The connector worker host address. (Required)")

	createCommand := flag.NewFlagSet("Config", flag.ExitOnError)
	configJson := createCommand.String("config", "", "The connector configuration. (Required)")
	configPort := createCommand.Int("port", 8083, "The connector worker port.")
	configHost := createCommand.String("host", "localhost", "The connector worker host address.")

	scaleCommand := flag.NewFlagSet("Scale", flag.ExitOnError)
	scaleName := scaleCommand.String("connector", "", "The connector name. (Required)")
	scaleTasks := scaleCommand.String("tasks-max", "", "The max number of tasks to update. (Required)")
	scalePort := scaleCommand.Int("port", 8083, "The connector worker port.")
	scaleHost := scaleCommand.String("host", "localhost", "The connector worker host address.")

	if len(os.Args) < 2 {
		usage()
	}

	command := os.Args[1]
	var connector string

	var client connect.ConnectRestClient
	switch command {
	case "config", "status", "delete", "resume", "pause", "tasks", "restart-failed":
		connectorCommand.Parse(os.Args[2:])
		connector = *connectName
		client = connect.NewConnectClient(*connectHost, *connectPort)
	case "list":
		listCommand.Parse(os.Args[2:])
		client = connect.NewConnectClient(*listHost, *listPort)
	case "delete-all", "plugins", "version":
		commonCommand.Parse(os.Args[2:])
		client = connect.NewConnectClient(*commonHost, *commonPort)
	case "create":
		createCommand.Parse(os.Args[2:])
		client = connect.NewConnectClient(*configHost, *configPort)
	case "scale":
		scaleCommand.Parse(os.Args[2:])
		connector = *scaleName
		client = connect.NewConnectClient(*scaleHost, *scalePort)
	case "help":
		switch os.Args[2] {
		case "config", "status", "delete", "resume", "pause", "tasks", "restart-failed":
			connectorCommand.PrintDefaults()
		case "create":
			createCommand.PrintDefaults()
		case "scale":
			scaleCommand.PrintDefaults()
		case "list":
			listCommand.PrintDefaults()
		case "delete-all", "plugins", "version":
			commonCommand.PrintDefaults()
		default:
			fmt.Println("Unknown help command `" + os.Args[2] + "`.  Run '" + os.Args[0] + " help'.")
		}
	default:
		usage()
	}

	if connectorCommand.Parsed() {
		if connector == "" {
			fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
			connectorCommand.PrintDefaults()
			os.Exit(1)
		}
		connectRegex := regexp.MustCompile(connector)
		matchConnectors := findMatchingConnectors(client, func(conn string) bool { return connectRegex.MatchString(conn) })
		for _, conn := range matchConnectors {
			switch command {
			case "config":
				fmt.Println(client.GetConfig(conn))
			case "status":
				fmt.Println(client.Status(conn))
			case "delete":
				client.Delete(conn)
			case "resume":
				client.Resume(conn)
			case "pause":
				client.Pause(conn)
			case "tasks":
				fmt.Println(client.Tasks(conn))
			case "restart-failed":
				connectStatus := getUnmarshalConnectStatus(client, conn)
				for _, task := range connectStatus.Tasks {
					if task.State == "FAILED" {
						client.Restart(connectStatus.Name, task.ID)
					}
				}
			}
		}
	}

	if listCommand.Parsed() {
		state := strings.ToUpper(*listState)
		switch state {
		case "RUNNING", "FAILED", "PAUSED", "UNASSIGNED":
			connectors := findMatchingConnectors(client, func(conn string) bool {
				connectStatus := getUnmarshalConnectStatus(client, conn)
				res := connectStatus.Connector.State == state
				for _, task := range connectStatus.Tasks {
					res = res || task.State == state
				}
				return res
			})
			fmt.Println(connectors)

		default:
			fmt.Println(client.List())
		}
	}

	if createCommand.Parsed() {
		client.Create(*configJson)
	}

	if scaleCommand.Parsed() {
		if connector == "" || *scaleTasks == "" {
			fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
			scaleCommand.PrintDefaults()
			os.Exit(1)
		}
		config := client.GetConfig(*scaleName)
		var connectConfig connect.ConnectorConfig
		err := json.Unmarshal([]byte(config), &connectConfig)
		if err != nil {
			panic(err)
		}
		connectConfig.Config["tasks.max"] = *scaleTasks
		jsonConfig, _ := json.Marshal(connectConfig.Config)
		fmt.Println(client.Update(*scaleName, string(jsonConfig)))
	}

	if commonCommand.Parsed() {
		switch command {
		case "version":
			fmt.Println(client.Version())
		case "plugins":
			fmt.Println(client.Plugins())
		case "delete-all":
			matchConnectors := findMatchingConnectors(client, func(_ string) bool { return true })
			for _, conn := range matchConnectors {
				client.Delete(conn)
			}
		}
	}
	os.Exit(0)
}
