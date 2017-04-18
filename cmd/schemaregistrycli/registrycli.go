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
	"flag"
	"fmt"
	"github.com/fhussonnois/kafkacli/registry"
	"github.com/fhussonnois/kafkacli/utils"
	"os"
)

// Display commands usage and exit with return code 1.
func usage() {
	fmt.Println("A simple Command line interface (CLI) to manage Confluent Schema Registry.\n")
	fmt.Fprintf(os.Stderr, "Usage of %s: command [arguments] \n", os.Args[0])
	fmt.Println("The commands are : \n")
	fmt.Println("	global-compatibility	Getting the global compatibility level.")
	fmt.Println("	compatibility		Getting subject compatibility level for a subject.")
	fmt.Println("	schema			Getting a specific version of the schema registered under this subject")
	fmt.Println("	register		Registering a new schema under the specified subject.")
	fmt.Println("	subjects		Getting the list of registered subjects.")
	fmt.Println("	test			Testing schemas for compatibility against specific versions of a subject’s schema.")
	fmt.Println("	set-compatibility	Setting a new compatibility level.")
	fmt.Println("	versions		Getting a list of versions registered under the specified subject.")
	fmt.Println("\nUse \"schemaregistrycli help [command]\" for more information about that command.")
	os.Exit(1)
}

func checkNotNull(arg string, fs *flag.FlagSet) {
	if arg == "" {
		fs.PrintDefaults()
		os.Exit(1)
	}
}

const (
	DEFAULT_HOST 	= "localhost"
	DEFAULT_PORT 	= 8081
	DEFAULT_VERSION = "latest"
)

type CommandArgs struct {
	host          *string
	port          *int
	subject       *string
	pretty        *bool
	version       *string
	schema        *string
	compatibility *string
}

type ArgParser struct {
	Args CommandArgs
	Flag *flag.FlagSet
}

func NewArgParser(name string) ArgParser {
	return ArgParser{
		Args: CommandArgs{},
		Flag: flag.NewFlagSet(name, flag.ExitOnError),
	}
}

func (p *ArgParser) withPortArg() ArgParser {
	p.Args.port = p.Flag.Int("port", DEFAULT_PORT, "The schema registry port. (Required)")
	return *p
}
func (p *ArgParser) withHostArg() ArgParser {
	p.Args.host = p.Flag.String("host", DEFAULT_HOST, "The schema registry hostname. (Required)")
	return *p
}
func (p *ArgParser) withPrettyArg() ArgParser {
	p.Args.pretty = p.Flag.Bool("pretty", false, "Pretty print json output.")
	return *p
}

func (p *ArgParser) withVersionArg() ArgParser {
	p.Args.version = p.Flag.String("version", DEFAULT_VERSION, "Version of the schema to be returned or the string \"latest\".")
	return *p
}

func (p *ArgParser) withSubjectArg() ArgParser {
	p.Args.subject = p.Flag.String("subject", "", "The name of the subject. (Required)")
	return *p
}

func (p *ArgParser) withSchemaArg() ArgParser {
	p.Args.schema = p.Flag.String("schema", "", "The Avro schema.")
	return *p
}

func (p *ArgParser) withCompatibilityArg() ArgParser {
	p.Args.compatibility = p.Flag.String("level", "", "The new compatibility level. Must be one of NONE, FULL, FORWARD, BACKWARD (Required)")
	return *p
}

func (p *ArgParser) parse(args []string) CommandArgs {
	p.Flag.Parse(args)
	return p.Args
}

func (p *ArgParser) withCommonArgs() {
	p.withHostArg()
	p.withPortArg()
	p.withPrettyArg()
}

func main() {

	if len(os.Args) < 2 {
		usage()
	}

	CommonArgParser := NewArgParser("ServerArgParser")
	CommonArgParser.withCommonArgs()

	SubjectArgParser := NewArgParser("SubjectArgParser")
	SubjectArgParser.withCommonArgs()
	SubjectArgParser.withSubjectArg()

	RegisterArgParser := NewArgParser("RegisterArgParser")
	RegisterArgParser.withCommonArgs()
	RegisterArgParser.withSubjectArg()
	RegisterArgParser.withSchemaArg()

	SchemaArgParser := NewArgParser("SchemaArgParser")
	SchemaArgParser.withCommonArgs()
	SchemaArgParser.withSubjectArg()
	SchemaArgParser.withVersionArg()

	CompatibilityArgParser := NewArgParser("CompatibilityArgParser")
	CompatibilityArgParser.withCommonArgs()
	CompatibilityArgParser.withSubjectArg()
	CompatibilityArgParser.withCompatibilityArg()

	TestCompatibilityArgParser := NewArgParser("TestCompatibilityArgParser")
	TestCompatibilityArgParser.withCommonArgs()
	TestCompatibilityArgParser.withSubjectArg()
	TestCompatibilityArgParser.withVersionArg()

	command := os.Args[1]
	var commandArgParser ArgParser
	switch command {
	case "subjects", "global-compatibility":
		commandArgParser = CommonArgParser
	case "versions", "compatibility":
		commandArgParser = SubjectArgParser
	case "set-compatibility":
		commandArgParser = CompatibilityArgParser
	case "schema":
		commandArgParser = SchemaArgParser
	case "register":
		commandArgParser = RegisterArgParser
	case "test":
		commandArgParser = TestCompatibilityArgParser
	case "help":
		switch os.Args[2] {
		case "subjects", "global-compatibility":
			CommonArgParser.Flag.PrintDefaults()
		case "versions", "compatibility":
			SubjectArgParser.Flag.PrintDefaults()
		case "schema":
			SchemaArgParser.Flag.PrintDefaults()
		case "register":
			RegisterArgParser.Flag.PrintDefaults()
		case "set-compatibility":
			CompatibilityArgParser.Flag.PrintDefaults()
		case "test":
			TestCompatibilityArgParser.Flag.PrintDefaults()
		default:
			fmt.Println("Unknown help command `" + os.Args[2] + "`.  Run '" + os.Args[0] + " help'.")
		}
		os.Exit(1)
	default:
		usage()
	}

	args := commandArgParser.parse(os.Args[2:])

	client := registry.NewRegistryClient(*args.host, *args.port)

	if CommonArgParser.Flag.Parsed() {
		switch command {
		case "subjects":
			utils.PrintJson(client.Subjects(), *args.pretty)
		case "global-compatibility":
			utils.PrintJson(client.GetGlobalCompatibility(), *args.pretty)
		}
	}
	if SubjectArgParser.Flag.Parsed() {
		switch command {
		case "versions":
			utils.PrintJson(client.Versions(*args.subject), *args.pretty)
		case "compatibility":
			utils.PrintJson(client.GetSubjectCompatibility(*args.subject), *args.pretty)
		}
	}
	if RegisterArgParser.Flag.Parsed() {
		switch command {
		case "register":
			schema := registry.Schema{Value: *args.schema}
			utils.PrintJson(client.Register(*args.subject, schema), *args.pretty)
		}
	}
	if SchemaArgParser.Flag.Parsed() {
		switch command {
		case "schema":
			utils.PrintJson(client.GetSubjectVersion(*args.subject, *args.version), *args.pretty)
		}
	}
	if TestCompatibilityArgParser.Flag.Parsed() {
		switch command {
		case "test":
			schema := registry.Schema{Value: *args.schema}
			utils.PrintJson(client.CheckSubjectCompatibility(*args.subject, *args.version, schema), *args.pretty)
		}
	}
	if CompatibilityArgParser.Flag.Parsed() {
		switch command {
		case "set-compatibility":
			compatibility := registry.Compatibility{Value: *args.compatibility}
			utils.PrintJson(client.UpdateSubjectCompatibility(*args.subject, compatibility), *args.pretty)
		}
	}

	os.Exit(0)
}
