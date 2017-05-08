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
	"sort"
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
	fmt.Fprintf(os.Stdin, "Usage of %s: command [arguments] \n", os.Args[0])
	fmt.Println("The commands are : \n")
	keys := []string{}
	for k := range Commands {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("	%-25s%s\n", k, Commands[k])
	}
	fmt.Println("\nUse \"kafka-connect-cli help [command]\" for more information about that command.")
	os.Exit(1)
}

const (
	KAFKA_CONNECT_HOST_ENV = "KAFKA_CONNECT_HOST"
	KAFKA_CONNECT_PORT_ENV = "KAFKA_CONNECT_PORT"
	DEFAULT_PORT           = "8083"
	DEFAULT_HOST           = "localhost"
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
	defaultPort, _ := strconv.Atoi(utils.GetUserLocalVarOrElse(KAFKA_CONNECT_PORT_ENV, DEFAULT_PORT))
	p.Args.port = p.Flag.Int("port", defaultPort, "The connector worker port. (Required)")
	return p
}
func (p *ArgParser) withHostArg() *ArgParser {
	defaultHost := utils.GetUserLocalVarOrElse(KAFKA_CONNECT_HOST_ENV, DEFAULT_HOST)
	p.Args.host = p.Flag.String("host", defaultHost, "The connector worker host address. (Required)")
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

	UpdateArgParser := NewArgParser("CreateArgParser")
	UpdateArgParser.withCommonArgs().withConnectorArg().withConfigArg()

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
	case "update":
		commandArgParser = UpdateArgParser
	case "scale":
		commandArgParser = ScaleArgParser
	case "help":
		if len(os.Args) < 3 {
			usage()
		}
		subCommand := os.Args[2]
		fmt.Printf("Usage of %s: %s\nThe arguments are :\n", subCommand, Commands[subCommand])
		switch subCommand {
		case "config", "status", "delete", "resume", "pause", "tasks", "restart-failed":
			ConnectorArgParser.Flag.PrintDefaults()
		case "create":
			CreateArgParser.Flag.PrintDefaults()
		case "scale":
			ScaleArgParser.Flag.PrintDefaults()
		case "update":
			UpdateArgParser.Flag.PrintDefaults()
		case "list":
			ListArgParser.Flag.PrintDefaults()
		case "delete-all", "plugins", "version":
			CommonArgParser.Flag.PrintDefaults()
		default:
			fmt.Fprint(os.Stderr, "Unknown help command `"+subCommand+"`.  Run '"+os.Args[0]+" help'.\n")
		}
		os.Exit(1)
	default:
		usage()
	}

	args := commandArgParser.parse(os.Args[2:])
	commandArgParser.Validates()

	client := connect.NewConnectClient(*args.host, *args.port)

	var err error
	var result interface{}

	if ConnectorArgParser.Flag.Parsed() {
		result, err = handleConnectorCommands(client, command, *args.connector)
	}

	if ListArgParser.Flag.Parsed() {
		result, err = handleListCommand(client, *args.state)
	}

	if CreateArgParser.Flag.Parsed() {
		result, err = handleCreateCommand(client, args)
	}

	if UpdateArgParser.Flag.Parsed() {
		result, err = handleUpdateCommand(client, args)
	}

	if ScaleArgParser.Flag.Parsed() {
		result, err = handleScaleCommand(client, *args.connector, *args.tasks)
	}

	if CommonArgParser.Flag.Parsed() {
		result, err = handleCommonsCommand(command, client)
	}
	printOutputAndExit(result, err, *args.pretty)
}

// handleConnectorCommands executes all connectors commands.
func handleConnectorCommands(client connect.ConnectRestClient, command string, connector string) (result interface{}, e error) {
	connectRegex := regexp.MustCompile(connector)
	matches, e := findMatchingConnectors(client, func(conn string) (bool, error) { return connectRegex.MatchString(conn), nil })
	if e == nil {
		if len(matches) == 0 {
			fmt.Fprintf(os.Stdin, "No matching connector found for '%s' \n", connector)
		}
		for _, conn := range matches {
			switch command {
			case "config":
				result, e = client.GetConfig(conn)
			case "status":
				result, e = client.Status(conn)
			case "delete":
				e = deleteConnector(client, conn)
			case "resume":
				e = client.Resume(conn)
				if e == nil {
					fmt.Fprintf(os.Stdin, "Successfully resumed connector %s \n", conn)
				}
			case "pause":
				e = client.Pause(conn)
				if e == nil {
					fmt.Fprintf(os.Stdin, "Successfully paused connector %s \n", conn)
				}
			case "tasks":
				result, e = client.Tasks(conn)
			case "restart-failed":
				status, e := client.Status(conn)
				if e == nil {
					for _, task := range status.Tasks {
						if task.State == "FAILED" {
							client.Restart(status.Name, task.ID)
						}
					}
				}
			}
		}
	}
	return
}

// handleListCommand executes "list" command.
func handleListCommand(client connect.ConnectRestClient, state string) (result interface{}, e error) {
	state = strings.ToUpper(state)
	switch state {
	case "RUNNING", "FAILED", "PAUSED", "UNASSIGNED":
		result, e = findMatchingConnectors(client, func(conn string) (bool, error) {
			status, e := client.Status(conn)
			if e != nil {
				return false, e
			}

			res := status.Connector.State == state
			for _, task := range status.Tasks {
				res = res || task.State == state
			}
			return res, nil
		})
	default:
		result, e = client.List()
	}
	return
}

// handleCreateCommand executes "create" command.
func handleCreateCommand(client connect.ConnectRestClient, args CommandArgs) (result interface{}, e error) {
	result, e = client.Create(readConnectorConfig(args))
	return
}

// handleUpdateCommand executes "update" command.
func handleUpdateCommand(client connect.ConnectRestClient, args CommandArgs) (result interface{}, e error) {
	config := readConnectorConfig(args)
	result, e = client.Update(connect.ConnectorConfig{Name: *args.connector, Config: config.Config})
	return
}

// handleScaleCommand executes "scale" command.
func handleScaleCommand(client connect.ConnectRestClient, connector string, tasks int) (result interface{}, e error) {
	config, err := client.GetConfig(connector)
	if err != nil {
		return nil, err
	}
	config.Config["tasks.max"] = strconv.Itoa(tasks)
	result, e = client.Update(connect.ConnectorConfig{Name: connector, Config: config.Config})
	return
}

// handleCommonsCommand executes either "version", "plugin" or "delete-all" commands.
func handleCommonsCommand(command string, client connect.ConnectRestClient) (result interface{}, e error) {
	switch command {
	case "version":
		result, e = client.Version()
	case "plugins":
		result, e = client.Plugins()
	case "delete-all":
		connectors, e := client.List()
		if e == nil {
			for _, conn := range connectors {
				e = deleteConnector(client, conn)
				if e != nil {
					break
				}
			}
		}
	}
	return
}

// readConnectorConfig reads a connector configuration from the specified arguments.
// Returns the configuration as map value-pairs.
func readConnectorConfig(args CommandArgs) (config connect.ConnectorConfig) {
	jsonString := *args.json
	if jsonString != "" {
		e := json.Unmarshal([]byte(jsonString), &config)
		if e != nil {
			fmt.Fprintf(os.Stderr, "Invalid configuration - error: %v\n", e)
			os.Exit(1)
		}
	}
	jsonFile := *args.jsonFile
	if jsonFile != "" {
		file, err := ioutil.ReadFile(jsonFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while reading config file '%s': %v\n", jsonFile, err)
			os.Exit(1)
		}
		e := json.Unmarshal(file, &config)
		if e != nil {
			fmt.Fprintf(os.Stderr, "Invalid configuration - error: %v\n", e)
			os.Exit(1)
		}
	}
	propsFile := *args.propsFile
	if propsFile != "" {
		res, err := utils.ReadProps(propsFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while reading config file '%s': %v\n", propsFile, err)
			os.Exit(1)
		}
		name := res["name"]
		if name != "" {
			delete(res, "name")
		} else {
			fmt.Fprint(os.Stderr, "Missing required configuration field : 'name'")
			os.Exit(1)
		}
		config = connect.ConnectorConfig{Name: name, Config: res}
	}
	return
}

func deleteConnector(client connect.ConnectRestClient, connector string) (e error) {
	connectorTasks, e := client.GetConfig(connector)
	if e == nil {
		fmt.Fprintf(os.Stdin, "\nCurrent configuration for connector %s\n\n", connector)
		config, _ := json.Marshal(connect.ConnectorConfig{Name: connectorTasks.Name, Config: connectorTasks.Config})
		utils.PrintJson(string(config), true)
		fmt.Fprint(os.Stdin, "\nSave this to use as the `-config.json` option during rollback connector\n\n")
		e = client.Delete(connector)
		if e == nil {
			fmt.Fprintf(os.Stdin, "Successfully deleted connector %s \n", connector)
		}
	}
	return
}

type Matcher func(connectorName string) (bool, error)

func findMatchingConnectors(client connect.ConnectRestClient, matcher Matcher) (connectors []string, e error) {
	list, e := client.List()
	if e == nil {
		for _, conn := range list {
			matches, e := matcher(conn)
			if e != nil {
				break
			}
			if matches {
				connectors = append(connectors, conn)
			}
		}
	}
	return
}

func printOutputAndExit(result interface{}, err error, pretty bool) {
	if err != nil {
		utils.PrintJson(err.Error(), pretty)
		os.Exit(1)
	}

	if result != nil {
		utils.PrintJson(result, pretty)
		os.Exit(0)
	}
}
