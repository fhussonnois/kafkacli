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
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/fhussonnois/kafkacli/connect"
	"github.com/fhussonnois/kafkacli/utils"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var Commands = map[string]string{
	"list":           "Listing active connectors on a worker.",
	"config":         "Getting connector configuration.",
	"create":         "Creating a new connector.",
	"delete":         "Deleting a connector.",
	"delete-all":     "eleting all connectors.",
	"pause":          "Pausing a connector (useful if downtime is needed for the system the connector interacts with).",
	"plugins":        "Listing installed connectors plugins.",
	"resume":         "Restarting a connector.",
	"restart-failed": "Restarting failed tasks for a connector.",
	"status":         "Getting connector status.",
	"tasks":          "Getting tasks for a connector.",
	"scale":          "Scaling up/down the number of tasks for a connector.",
	"update":         "Updating connector configuration.",
	"version":        "Getting a connect worker version.",
}

// Display commands usage and exit with return code 1.
func usage() {
	fmt.Println("A simple Command line interface (CLI) to manage connectors through the Kafka Connect REST Interface.\n")
	fmt.Fprintf(os.Stderr, "Usage of %s: command [arguments] \n", os.Args[0])
	fmt.Println("The commands are : \n")
	for k, v := range Commands {
		fmt.Printf("	%-20s%s\n", k, v)
	}
	fmt.Println("\nUse \"kafka-connect-cli help [command]\" for more information about that command.")
	os.Exit(1)
}

const (
	DEFAULT_PORT = 8083
	DEFAULT_HOST = "localhost"
)

type CommandArgs struct {
	host      *string
	port      *int
	pretty    *bool
	connector *string
	state     *string
	json      *string
	jsonFile  *string
	propsFile *string
	tasks     *int
}

type Validator struct {
	message string
	apply   func(args CommandArgs) bool
}

type ArgParser struct {
	Args       CommandArgs
	Flag       *flag.FlagSet
	validators []Validator
}

func NewArgParser(name string) ArgParser {
	return ArgParser{
		Args: CommandArgs{},
		Flag: flag.NewFlagSet(name, flag.ExitOnError),
	}
}

func (p *ArgParser) Validates() {
	for _, v := range p.validators {
		if !v.apply(p.Args) {
			fmt.Fprintf(os.Stderr, "'%s'\n\n", v.message)
			fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
			p.Flag.PrintDefaults()
			os.Exit(1)
		}
	}
}

func (p *ArgParser) addValidators(v Validator) {
	p.validators = append(p.validators, v)
}

func (p *ArgParser) withPortArg() *ArgParser {
	p.Args.port = p.Flag.Int("port", DEFAULT_PORT, "The connector worker port. (Required)")
	return p
}
func (p *ArgParser) withHostArg() *ArgParser {
	p.Args.host = p.Flag.String("host", DEFAULT_HOST, "The connector worker host address. (Required)")
	return p
}
func (p *ArgParser) withPrettyArg() *ArgParser {
	p.Args.pretty = p.Flag.Bool("pretty", false, "Pretty print json output.")
	return p
}
func (p *ArgParser) withConnectorArg() *ArgParser {
	p.Args.connector = p.Flag.String("connector", "", "The connector name or a regex. (Required)")

	apply := func(args CommandArgs) bool { return *args.connector != "" }
	p.addValidators(Validator{message: "Missing or invalid argument 'connector'", apply: apply})
	return p
}
func (p *ArgParser) withStateArg() *ArgParser {
	p.Args.state = p.Flag.String("with-state", "", "Filter on connector/task for the specified state [running|failed|paused|unassigned]")
	return p
}
func (p *ArgParser) withConfigArg() *ArgParser {
	p.Args.json = p.Flag.String("config", "", "The connector configuration json string. (Required)")
	p.Args.jsonFile = p.Flag.String("config.json", "", "<file> The connector configuration json file. (Required)")
	p.Args.propsFile = p.Flag.String("config.props", "", "<file> The connector configuration properties file. (Required)")

	apply := func(args CommandArgs) bool { return *args.json != "" || *args.jsonFile != "" || *args.propsFile != "" }
	p.addValidators(Validator{message: "Missing or invalid arguments [config | config.json | config.props]", apply: apply})
	return p
}
func (p *ArgParser) withTasksMaxArg() *ArgParser {
	p.Args.tasks = p.Flag.Int("tasks-max", 0, "The max number of tasks to update. (Required)")
	apply := func(args CommandArgs) bool { return *args.tasks > 0 }
	p.addValidators(Validator{message: "Missing or invalid argument 'tasks-max'", apply: apply})
	return p
}
func (p *ArgParser) parse(args []string) CommandArgs {
	p.Flag.Parse(args)
	return p.Args
}

func (p *ArgParser) withCommonArgs() *ArgParser {
	p.withHostArg().withPortArg().withPrettyArg()
	return p
}

// A simple command line interface (CLI) to manage connectors though the Kafka Connect REST Interface.
func main() {

	if len(os.Args) < 2 {
		usage()
	}

	CommonArgParser := NewArgParser("WorkerArgParser")
	CommonArgParser.withCommonArgs()

	ConnectorArgParser := NewArgParser("ConnectorArgParser")
	ConnectorArgParser.withCommonArgs().withConnectorArg()

	ListArgParser := NewArgParser("ListArgParser")
	ListArgParser.withCommonArgs().withStateArg()

	CreateArgParser := NewArgParser("CreateArgParser")
	CreateArgParser.withCommonArgs().withConfigArg()

	ScaleArgParser := NewArgParser("ScaleArgParser")
	ScaleArgParser.withCommonArgs().withConnectorArg().withTasksMaxArg()

	command := os.Args[1]
	var commandArgParser ArgParser
	switch command {
	case "config", "status", "delete", "resume", "pause", "tasks", "restart-failed":
		commandArgParser = ConnectorArgParser
	case "list":
		commandArgParser = ListArgParser
	case "delete-all", "plugins", "version":
		commandArgParser = CommonArgParser
	case "create":
		commandArgParser = CreateArgParser
	case "scale":
		commandArgParser = ScaleArgParser
	case "help":
		fmt.Printf("Usage of %s: %s\nThe arguments are :\n", os.Args[2], Commands[os.Args[2]])
		switch os.Args[2] {
		case "config", "status", "delete", "resume", "pause", "tasks", "restart-failed":
			ConnectorArgParser.Flag.PrintDefaults()
		case "create":
			CreateArgParser.Flag.PrintDefaults()
		case "scale":
			ScaleArgParser.Flag.PrintDefaults()
		case "list":
			ListArgParser.Flag.PrintDefaults()
		case "delete-all", "plugins", "version":
			CommonArgParser.Flag.PrintDefaults()
		default:
			fmt.Println("Unknown help command `" + os.Args[2] + "`.  Run '" + os.Args[0] + " help'.")
		}
		os.Exit(1)
	default:
		usage()
	}

	args := commandArgParser.parse(os.Args[2:])
	commandArgParser.Validates()

	client := connect.NewConnectClient(*args.host, *args.port)

	if ConnectorArgParser.Flag.Parsed() {
		connector := *args.connector
		connectRegex := regexp.MustCompile(connector)
		matchConnectors := findMatchingConnectors(client, func(conn string) bool { return connectRegex.MatchString(conn) })
		for _, conn := range matchConnectors {
			switch command {
			case "config":
				utils.PrintJson(client.GetConfig(conn), *args.pretty)
			case "status":
				utils.PrintJson(client.Status(conn), *args.pretty)
			case "delete":
				client.Delete(conn)
			case "resume":
				client.Resume(conn)
			case "pause":
				client.Pause(conn)
			case "tasks":
				utils.PrintJson(client.Tasks(conn), *args.pretty)
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

	if ListArgParser.Flag.Parsed() {
		state := strings.ToUpper(*args.state)
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
			utils.PrintJson(connectors, *args.pretty)

		default:
			utils.PrintJson(client.List(), *args.pretty)
		}
	}

	if CreateArgParser.Flag.Parsed() {
		var jsonConfig []byte
		if *args.json != "" {
			jsonConfig = []byte(*args.json)
		}
		if *args.jsonFile != "" {
			file, err := ioutil.ReadFile(*args.jsonFile)
			if err != nil {
				fmt.Printf("Error while reading config file %s error: %v\n", *args.jsonFile, err)
				os.Exit(1)
			}
			jsonConfig = file
		}
		if *args.propsFile != "" {
			config, err := utils.ReadProps(*args.propsFile)
			if err != nil {
				fmt.Printf("Error while reading config file %s error: %v\n", *args.jsonFile, err)
				os.Exit(1)
			}
			name := config["name"]
			delete(config, "name")
			jsonConfig, _ = json.Marshal(connect.Config{Name: name, Config: config})
		}
		var config connect.Config
		fmt.Println(string(jsonConfig))
		err := json.Unmarshal(jsonConfig, &config)
		if err != nil {
			fmt.Printf("Invalid configuration - error: %v\n", err)
			os.Exit(1)
		}
		utils.PrintJson(client.Create(config), *args.pretty)
	}

	if ScaleArgParser.Flag.Parsed() {
		config := client.GetConfig(*args.connector)
		config.Config["tasks.max"] = strconv.Itoa(*args.tasks)
		jsonConfig, _ := json.Marshal(config.Config)
		utils.PrintJson(client.Update(*args.connector, string(jsonConfig)), *args.pretty)
	}

	if CommonArgParser.Flag.Parsed() {
		switch command {
		case "version":
			utils.PrintJson(client.Version(), *args.pretty)
		case "plugins":
			utils.PrintJson(client.Plugins(), *args.pretty)
		case "delete-all":
			matchConnectors := findMatchingConnectors(client, func(_ string) bool { return true })
			for _, conn := range matchConnectors {
				client.Delete(conn)
			}
		}
	}
	os.Exit(0)
}

func findMatchingConnectors(client connect.ConnectRestClient, fn func(string) bool) []string {
	var matchConnectors []string
	for _, conn := range client.List() {
		if fn(conn) {
			matchConnectors = append(matchConnectors, conn)
		}
	}
	return matchConnectors
}
