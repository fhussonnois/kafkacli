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
	fmt.Println("	subjects		Getting the list of registered subjects.")
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
	DEFAULT_HOST = "localhost"
	DEFAULT_PORT = 8081
)

func main() {

	if len(os.Args) < 2 {
		usage()
	}

	commonCommand := flag.NewFlagSet("Server", flag.ExitOnError)
	commonPort := commonCommand.Int("port", DEFAULT_PORT, "The schema registry port. (Required)")
	commonHost := commonCommand.String("host", DEFAULT_HOST, "The schema registry host address. (Required)")
	commonPretty := commonCommand.Bool("pretty", false, "Pretty print json output.")

	subjectCommand := flag.NewFlagSet("Subjects", flag.ExitOnError)
	subjectPort := subjectCommand.Int("port", DEFAULT_PORT, "The schema registry port. (Required)")
	subjectHost := subjectCommand.String("host", DEFAULT_HOST, "The schema registry host address. (Required)")
	subjectName := subjectCommand.String("subject", "", "The name of the subject. (Required)")
	subjectPretty := subjectCommand.Bool("pretty", false, "Pretty print json output.")

	specificCommand := flag.NewFlagSet("SpecificSubject", flag.ExitOnError)
	specificPort := specificCommand.Int("port", DEFAULT_PORT, "The schema registry port. (Required)")
	specificHost := specificCommand.String("host", DEFAULT_HOST, "The schema registry host address. (Required)")
	specificName := specificCommand.String("subject", "", "The name of the subject. (Required)")
	specificVersion := specificCommand.String("version", "latest", "Version of the schema to be returned or the string \"latest\".")
	specificPretty := specificCommand.Bool("pretty", false, "Pretty print json output.")

	command := os.Args[1]
	var subject string
	var pretty bool
	var version string
	var client registry.SchemaRegistryRestClient
	switch command {
	case "subjects", "global-compatibility":
		commonCommand.Parse(os.Args[2:])
		client = registry.NewRegistryClient(*commonHost, *commonPort)
		pretty = *commonPretty
	case "versions", "compatibility":
		subjectCommand.Parse(os.Args[2:])
		client = registry.NewRegistryClient(*subjectHost, *subjectPort)
		pretty = *subjectPretty
		subject = *subjectName
		checkNotNull(subject, subjectCommand)
	case "schema":
		specificCommand.Parse(os.Args[2:])
		client = registry.NewRegistryClient(*specificHost, *specificPort)
		pretty = *specificPretty
		subject = *specificName
		checkNotNull(subject, specificCommand)
		version = *specificVersion
		checkNotNull(version, specificCommand)
	case "help":
		switch os.Args[2] {
		case "subjects", "global-compatibility":
			commonCommand.PrintDefaults()
		default:
			fmt.Println("Unknown help command `" + os.Args[2] + "`.  Run '" + os.Args[0] + " help'.")
		}
	default:
		usage()
	}

	if commonCommand.Parsed() {
		switch command {
		case "subjects":
			utils.PrintJson(client.Subjects(), pretty)
		case "global-compatibility":
			utils.PrintJson(client.GetGlobalCompatibility(), pretty)
		}
	}

	if subjectCommand.Parsed() {
		switch command {
		case "versions":
			utils.PrintJson(client.Versions(subject), pretty)
		case "compatibility":
			utils.PrintJson(client.GetSubjectCompatibility(subject), pretty)
		}
	}

	if specificCommand.Parsed() {
		switch command {
		case "schema":
			utils.PrintJson(client.GetSubjectVersion(subject, version), pretty)
		}
	}

	os.Exit(0)
}
