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
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/fhussonnois/kafkacli/registry"
	"github.com/fhussonnois/kafkacli/utils"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
)

var Commands = map[string]string{

	"compatibility": "Getting subject compatibility level for a subject.",
	"exist":         "Checking if a schema has already been registered under the specified subject",
	"get":           "Getting a specific version of the schema registered under this subject",
	"global-compatibility": "Getting the global compatibility level.",
	"register":             "Registering a new schema under the specified subject.",
	"set-compatibility":    "Setting a new compatibility level.",
	"subjects":             "Getting the list of registered subjects.",
	"test":                 "Testing schemas for compatibility against specific versions of a subject’s schema.",
	"versions":             "Getting a list of versions registered under the specified subject.",
}

// Display commands usage and exit with return code 1.
func usage() {
	fmt.Println("A simple Command line interface (CLI) to manage Confluent Schema Registry.\n")
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
	fmt.Println("\nUse \"schema-registry-cli help [command]\" for more information about that command.")
	os.Exit(1)
}

const (
	SCHEMA_REGISTRY_HOST_ENV = "SCHEMA_REGISTRY_HOST"
	SCHEMA_REGISTRY_PORT_ENV = "SCHEMA_REGISTRY_PORT"
	DEFAULT_HOST             = "localhost"
	DEFAULT_PORT             = "8081"
	DEFAULT_VERSION          = "latest"
)

type CommandArgs struct {
	host          *string
	port          *int
	subject       *string
	pretty        *bool
	version       *string
	isSchema      *bool
	schemaString  *string
	schemaJson    *string
	schemaUrl     *string
	compatibility *string
	force         *bool
}

type ArgParser struct {
	Args       CommandArgs
	Flag       *flag.FlagSet
	validators []Validator
}

type Validator interface {
	Message() string
	Apply(args CommandArgs) bool
}

type CheckNotNull struct {
	name string
	arg  func(args CommandArgs) string
}

func (p CheckNotNull) Message() string             { return "Missing or invalid argument '" + p.name + "'" }
func (p CheckNotNull) Apply(args CommandArgs) bool { return len(p.arg(args)) > 0 }

type CheckValueIn struct {
	name   string
	values []string
	arg    func(args CommandArgs) string
}

func (p CheckValueIn) Message() string { return "Missing or invalid argument '" + p.name + "'" }
func (p CheckValueIn) Apply(args CommandArgs) (contains bool) {
	contains = false
	for _, v := range p.values {
		if p.arg(args) == v {
			contains = true
			return
		}
	}
	return
}

func (p *ArgParser) Validates() {
	for _, v := range p.validators {
		if !v.Apply(p.Args) {
			fmt.Fprintf(os.Stderr, "'%s'\n\n", v.Message())
			fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
			p.Flag.PrintDefaults()
			os.Exit(1)
		}
	}
}

func (p *ArgParser) addValidators(v Validator) {
	p.validators = append(p.validators, v)
}

func NewArgParser(name string) ArgParser {
	return ArgParser{
		Args: CommandArgs{},
		Flag: flag.NewFlagSet(name, flag.ExitOnError),
	}
}

func (p *ArgParser) withPortArg() *ArgParser {
	defaultPort, _ := strconv.Atoi(utils.GetUserLocalVarOrElse(SCHEMA_REGISTRY_PORT_ENV, DEFAULT_PORT))
	p.Args.port = p.Flag.Int("port", defaultPort, "The schema registry port. (Required)")
	p.addValidators(CheckNotNull{name: "port", arg: func(args CommandArgs) string { return strconv.Itoa(*args.port) }})
	return p
}
func (p *ArgParser) withHostArg() *ArgParser {
	defaultHost := utils.GetUserLocalVarOrElse(SCHEMA_REGISTRY_HOST_ENV, DEFAULT_HOST)
	p.Args.host = p.Flag.String("host", defaultHost, "The schema registry hostname. (Required)")
	p.addValidators(CheckNotNull{name: "host", arg: func(args CommandArgs) string { return *args.host }})
	return p
}
func (p *ArgParser) withPrettyArg() *ArgParser {
	p.Args.pretty = p.Flag.Bool("pretty", false, "Pretty print json output.")
	return p
}

func (p *ArgParser) withVersionArg() *ArgParser {
	p.Args.version = p.Flag.String("version", DEFAULT_VERSION, "Version of the schema to be returned or the string \"latest\".")
	return p
}

func (p *ArgParser) withSubjectArg() *ArgParser {
	p.Args.subject = p.Flag.String("subject", "", "The name of the subject (Required).")
	p.addValidators(CheckNotNull{name: "subject", arg: func(args CommandArgs) string { return *args.subject }})
	return p
}

func (p *ArgParser) withSchemaArg() *ArgParser {
	p.Args.schemaString = p.Flag.String("schema", "", "The Avro schema json string (Required).")
	p.Args.schemaJson = p.Flag.String("schema.json", "", "<file> The Avro schema json file (Required).")
	p.Args.schemaUrl = p.Flag.String("schema.url", "", "<url> The Avro schema json url (Required).")
	return p
}

func (p *ArgParser) withIsSchemaArg() *ArgParser {
	p.Args.isSchema = p.Flag.Bool("schema", false, "Retrieve only the json schema from the version.")
	return p
}

func (p *ArgParser) withForceArg() *ArgParser {
	p.Args.force = p.Flag.Bool("force", false, "Temporally set the subject compatibility-level to NONE before registering schema.")
	return p
}

func (p *ArgParser) withCompatibilityArg() *ArgParser {
	values := []string{"NONE", "FULL", "FORWARD", "BACKWARD"}
	p.Args.compatibility = p.Flag.String("level", "", "The new compatibility level. Must be one of "+strings.Join(values, ",")+" (Required)")
	p.addValidators(CheckNotNull{name: "level", arg: func(args CommandArgs) string { return *args.compatibility }})
	p.addValidators(CheckValueIn{name: "level", arg: func(args CommandArgs) string { return *args.compatibility }, values: values})
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

func main() {

	if len(os.Args) < 2 {
		usage()
	}

	CommonArgParser := NewArgParser("ServerArgParser")
	CommonArgParser.withCommonArgs()

	SubjectArgParser := NewArgParser("SubjectArgParser")
	SubjectArgParser.withCommonArgs().withSubjectArg()

	RegisterArgParser := NewArgParser("RegisterArgParser")
	RegisterArgParser.withCommonArgs().withSubjectArg().withSchemaArg().withForceArg()

	ExistArgParser := NewArgParser("ExistArgParser")
	ExistArgParser.withCommonArgs().withSubjectArg().withSchemaArg()

	SchemaArgParser := NewArgParser("SchemaArgParser")
	SchemaArgParser.withCommonArgs().withSubjectArg().withVersionArg().withIsSchemaArg()

	CompatibilityArgParser := NewArgParser("CompatibilityArgParser")
	CompatibilityArgParser.withCommonArgs().withSubjectArg().withCompatibilityArg()

	TestCompatibilityArgParser := NewArgParser("TestCompatibilityArgParser")
	TestCompatibilityArgParser.withCommonArgs().withSubjectArg().withVersionArg().withSchemaArg()

	command := os.Args[1]
	var commandArgParser ArgParser
	switch command {
	case "subjects", "global-compatibility":
		commandArgParser = CommonArgParser
	case "versions", "compatibility":
		commandArgParser = SubjectArgParser
	case "set-compatibility":
		commandArgParser = CompatibilityArgParser
	case "get":
		commandArgParser = SchemaArgParser
	case "exists":
		commandArgParser = ExistArgParser
	case "register":
		commandArgParser = RegisterArgParser
	case "test":
		commandArgParser = TestCompatibilityArgParser
	case "help":
		if len(os.Args) < 3 {
			usage()
		}
		subCommand := os.Args[2]
		fmt.Printf("Usage of %s: %s\nThe arguments are :\n", subCommand, Commands[subCommand])
		switch subCommand {
		case "subjects", "global-compatibility":
			CommonArgParser.Flag.PrintDefaults()
		case "versions", "compatibility":
			SubjectArgParser.Flag.PrintDefaults()
		case "get":
			SchemaArgParser.Flag.PrintDefaults()
		case "exists":
			ExistArgParser.Flag.PrintDefaults()
		case "register":
			RegisterArgParser.Flag.PrintDefaults()
		case "set-compatibility":
			CompatibilityArgParser.Flag.PrintDefaults()
		case "test":
			TestCompatibilityArgParser.Flag.PrintDefaults()
		default:
			fmt.Fprint(os.Stderr, "Unknown help command `"+subCommand+"`.  Run '"+os.Args[0]+" help'.\n")
		}
		os.Exit(1)
	default:
		usage()
	}

	args := commandArgParser.parse(os.Args[2:])
	commandArgParser.Validates()

	client := registry.NewRegistryClient(*args.host, *args.port)

	if CommonArgParser.Flag.Parsed() {
		switch command {
		case "subjects":
			res, err := client.Subjects()
			if err != nil {
				utils.PrintJson(err.Error(), *args.pretty)
			} else {
				utils.PrintJson(res, *args.pretty)
			}
		case "global-compatibility":
			res, err := client.GetGlobalCompatibility()
			if err != nil {
				utils.PrintJson(err.Error(), *args.pretty)
			} else {
				utils.PrintJson(res, *args.pretty)
			}

		}
	}
	if SubjectArgParser.Flag.Parsed() {
		switch command {
		case "versions":
			res, err := client.Versions(*args.subject)
			if err != nil {
				utils.PrintJson(err.Error(), *args.pretty)
			} else {
				utils.PrintJson(res, *args.pretty)
			}
		case "compatibility":
			res, err := client.GetSubjectCompatibility(*args.subject)
			if err != nil {
				utils.PrintJson(err.Error(), *args.pretty)
			} else {
				utils.PrintJson(res, *args.pretty)
			}
		}
	}
	if RegisterArgParser.Flag.Parsed() {
		switch command {
		case "register":
			schema := evaluateSchemaArg(args)
			var compatibilityLevel string
			if *args.force {
				compatibility, err := client.GetSubjectCompatibility(*args.subject)
				if err == nil {
					compatibilityLevel = compatibility.Value
					if compatibilityLevel != "NONE" {
						client.UpdateSubjectCompatibility(*args.subject, registry.Compatibility{Value: "NONE"})
					}
				}
			}

			res, err := client.Register(*args.subject, schema)
			if err != nil {
				utils.PrintJson(err.Error(), *args.pretty)
			} else {
				utils.PrintJson(res, *args.pretty)
			}

			if *args.force && len(compatibilityLevel) > 0 && compatibilityLevel != "NONE" {
				client.UpdateSubjectCompatibility(*args.subject, registry.Compatibility{Value: compatibilityLevel})
			}
		}
	}
	if ExistArgParser.Flag.Parsed() {
		switch command {
		case "exists":
			schema := evaluateSchemaArg(args)
			res, err := client.Exists(*args.subject, schema)
			if err != nil {
				utils.PrintJson(err.Error(), *args.pretty)
			} else {
				utils.PrintJson(res, *args.pretty)
			}
		}
	}
	if SchemaArgParser.Flag.Parsed() {
		switch command {
		case "get":
			version, err := client.GetSubjectVersion(*args.subject, *args.version)
			if err != nil {
				utils.PrintJson(err.Error(), *args.pretty)
			} else if *args.isSchema {
				s := strings.Replace(version.Schema, "\\", "", -1)
				utils.PrintJson(s, *args.pretty)
			} else {
				utils.PrintJson(version, *args.pretty)
			}
		}
	}
	if TestCompatibilityArgParser.Flag.Parsed() {
		switch command {
		case "test":
			schema := evaluateSchemaArg(args)
			res, err := client.CheckSubjectCompatibility(*args.subject, *args.version, schema)
			if err != nil {
				utils.PrintJson(err.Error(), *args.pretty)
			} else {
				utils.PrintJson(res, *args.pretty)
			}
		}
	}
	if CompatibilityArgParser.Flag.Parsed() {
		switch command {
		case "set-compatibility":
			compatibility := registry.Compatibility{Value: *args.compatibility}
			res, err := client.UpdateSubjectCompatibility(*args.subject, compatibility)
			if err != nil {
				utils.PrintJson(err.Error(), *args.pretty)
			} else {
				utils.PrintJson(res, *args.pretty)
			}
		}
	}
	os.Exit(0)
}

// Default interface to read a JSON Schema
type SchemaReader interface {
	Read(source string) (*registry.Schema, error)
}

// JSONSchemaReader implementation to read Schema from json string.
type JSONSchemaReader struct {
}

func (reader JSONSchemaReader) Read(source string) (*registry.Schema, error) {
	return &registry.Schema{Value: source}, nil
}

// FileSchemaReader implementation  to read Schema from file string.
type FileSchemaReader struct {
	JsonReader JSONSchemaReader
}

func (reader FileSchemaReader) Read(source string) (*registry.Schema, error) {
	file, err := ioutil.ReadFile(source)
	if err != nil {
		return nil, errors.New(string("Error while reading config file " + source + " error: " + err.Error()))
	}
	return reader.JsonReader.Read(string(file))
}

// HTTPSchemaReader implementation  to read Schema from file string.
type HTTPSchemaReader struct {
	JsonReader JSONSchemaReader
}

func (reader HTTPSchemaReader) Read(source string) (*registry.Schema, error) {
	req, err := http.NewRequest("GET", source, bytes.NewBufferString(""))
	req.Header.Add("Accept", "application/json")
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
	return reader.JsonReader.Read(string(body))
}

func evaluateSchemaArg(args CommandArgs) registry.Schema {

	var source string
	var reader SchemaReader
	if *args.schemaString != "" {
		reader = JSONSchemaReader{}
		source = *args.schemaString
	}
	if *args.schemaJson != "" {
		reader = FileSchemaReader{}
		source = *args.schemaJson
	}
	if *args.schemaUrl != "" {
		reader = HTTPSchemaReader{}
		source = *args.schemaUrl
	}

	schema, err := reader.Read(source)
	if err != nil {
		panic(err)
	}
	return *schema
}
