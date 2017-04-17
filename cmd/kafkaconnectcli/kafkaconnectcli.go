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
	"github.com/fhussonnois/kafkacli/utils"
	"os"
	"regexp"
	"strings"
)

type Matcher func(string) bool

func findMatchingConnectors(client connect.ConnectRestClient, fn Matcher) []string {
	var matchConnectors []string
	for _, conn := range client.List() {
		if fn(conn) {
			matchConnectors = append(matchConnectors, conn)
		}
	}
	return matchConnectors
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
	commonPretty := commonCommand.Bool("pretty", false, "Pretty print json output.")

	connectorCommand := flag.NewFlagSet("Connector", flag.ExitOnError)
	connectName := connectorCommand.String("connector", "", "The connector name or a regex. (Required)")
	connectPort := connectorCommand.Int("port", 8083, "The connector worker port.")
	connectHost := connectorCommand.String("host", "localhost", "The connector worker host address.")
	connectPretty := connectorCommand.Bool("pretty", false, "Pretty print json output.")

	listCommand := flag.NewFlagSet("List", flag.ExitOnError)
	listState := listCommand.String("with-state", "", "Filter on connector/task for the specified state [running|failed|paused|unassigned]")
	listPort := listCommand.Int("port", 8083, "The connector worker port. (Required)")
	listHost := listCommand.String("host", "localhost", "The connector worker host address. (Required)")
	listPretty := listCommand.Bool("pretty", false, "Pretty print json output.")

	createCommand := flag.NewFlagSet("Config", flag.ExitOnError)
	createJson := createCommand.String("config", "", "The connector configuration. (Required)")
	createPort := createCommand.Int("port", 8083, "The connector worker port.")
	createHost := createCommand.String("host", "localhost", "The connector worker host address.")
	createPretty := createCommand.Bool("pretty", false, "Pretty print json output.")

	scaleCommand := flag.NewFlagSet("Scale", flag.ExitOnError)
	scaleName := scaleCommand.String("connector", "", "The connector name. (Required)")
	scaleTasks := scaleCommand.String("tasks-max", "", "The max number of tasks to update. (Required)")
	scalePort := scaleCommand.Int("port", 8083, "The connector worker port.")
	scaleHost := scaleCommand.String("host", "localhost", "The connector worker host address.")
	scalePretty := scaleCommand.Bool("pretty", false, "Pretty print json output.")

	if len(os.Args) < 2 {
		usage()
	}

	command := os.Args[1]
	var connector string
	var pretty bool
	var client connect.ConnectRestClient
	switch command {
	case "config", "status", "delete", "resume", "pause", "tasks", "restart-failed":
		connectorCommand.Parse(os.Args[2:])
		client = connect.NewConnectClient(*connectHost, *connectPort)
		connector = *connectName
		pretty = *connectPretty
	case "list":
		listCommand.Parse(os.Args[2:])
		client = connect.NewConnectClient(*listHost, *listPort)
		pretty = *listPretty
	case "delete-all", "plugins", "version":
		commonCommand.Parse(os.Args[2:])
		client = connect.NewConnectClient(*commonHost, *commonPort)
		pretty = *commonPretty
	case "create":
		createCommand.Parse(os.Args[2:])
		client = connect.NewConnectClient(*createHost, *createPort)
		pretty = *createPretty
	case "scale":
		scaleCommand.Parse(os.Args[2:])
		client = connect.NewConnectClient(*scaleHost, *scalePort)
		connector = *scaleName
		pretty = *scalePretty

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
				utils.PrintJson(client.GetConfig(conn), pretty)
			case "status":
				utils.PrintJson(client.Status(conn), pretty)
			case "delete":
				client.Delete(conn)
			case "resume":
				client.Resume(conn)
			case "pause":
				client.Pause(conn)
			case "tasks":
				utils.PrintJson(client.Tasks(conn), pretty)
			case "restart-failed":
				status := client.Status(conn)
				for _, task := range status.Tasks {
					if task.State == "FAILED" {
						client.Restart(status.Name, task.ID)
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
				status := client.Status(conn)
				res := status.Connector.State == state
				for _, task := range status.Tasks {
					res = res || task.State == state
				}
				return res
			})
			utils.PrintJson(connectors, pretty)

		default:
			utils.PrintJson(client.List(), pretty)
		}
	}

	if createCommand.Parsed() {
		utils.PrintJson(client.Create(*createJson), pretty)
	}

	if scaleCommand.Parsed() {
		if connector == "" || *scaleTasks == "" {
			fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
			scaleCommand.PrintDefaults()
			os.Exit(1)
		}
		config := client.GetConfig(*scaleName)
		config.Config["tasks.max"] = *scaleTasks
		jsonConfig, _ := json.Marshal(config.Config)
		utils.PrintJson(client.Update(*scaleName, string(jsonConfig)), pretty)
	}

	if commonCommand.Parsed() {
		switch command {
		case "version":
			utils.PrintJson(client.Version(), pretty)
		case "plugins":
			utils.PrintJson(client.Plugins(), pretty)
		case "delete-all":
			matchConnectors := findMatchingConnectors(client, func(_ string) bool { return true })
			for _, conn := range matchConnectors {
				client.Delete(conn)
			}
		}
	}
	os.Exit(0)
}
