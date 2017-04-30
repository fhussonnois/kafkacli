Kafka CLI
=============================================================

[![Build Status](https://travis-ci.org/fhussonnois/kafkacli.svg?branch=master)](https://travis-ci.org/fhussonnois/kafkacli)
[![GoDoc](https://img.shields.io/badge/docs-GoDoc-blue.svg)](https://godoc.org/github.com/fhussonnois/kafkacli)

[Apache Kafka](http://kafka.apache.org/) is a high-throughput, distributed, publish-subscribe messaging system.

##  Requirements
------------

1. [Kafka 0.9.0.* or latest releases](http://kafka.apache.org/downloads.html)
2. [Confluent 3.0.* or latest releases](https://www.confluent.io/download/)

## How to build project ?

[Go Lang](https://golang.org/doc/install) must be installed in order to build the project.

You can used the command ```go get``` can be used to retrieve and build the project directly from GitHub.

```bash
go get github.com/fhussonnois/kafkacli/cmd/...
```

The script ```./release.sh``` can be used to package project tar.gz for all OS/architectures.

## Setting defaults Host/Port for use by the CLI

Setting your host/port for use by the CLI tools can be done in a number of ways, here are some approaches:

* Set the HOST and PORT environment variables.

  To set these variables on Linux, macOS, or Unix, use export:

  ```
  export SCHEMA_REGISTRY_HOST=your_schema_registry_host
  export SCHEMA_REGISTRY_PORT=your_schema_registry_port
  ```

  ```
  export KAFKA_CONNECT_HOST=your_kafka_connect_host
  export KAFKA_CONNECT_PORT=your_kafka_connect_port
  ```

* Set the HOST and PORT in the CLI profile properties file on your local system, located at:
  * ~/.kafkacli/host on Linux, macOS, or Unix

  This file should contain lines in the following format:
  ```
  # Kafka Connect
  kafka_connect_host = your_kafka_connect_host
  kafka_connect_port = your_kafka_connect_port

  # Schema Registry
  schema_registry_host = your_schema_registry_host
  schema_registry_port = your_schema_registry_port
  ```
* Set the HOST and PORT while specifying command to run :

The CLI commands accept both arguments `-host` and `-port`

## Kafka Connect CLI

A simple Command line interface (CLI) to manage connectors through the Kafka Connect REST Interface.

### Run command
```bash
./bin/kafka-connect-cli
```

### Usage

```
A simple Command line interface (CLI) to manage connectors through the Kafka Connect REST Interface.

Usage of ./bin/kafka-connect-cli: command [arguments]
The commands are :

    list            Listing active connectors on a worker.
    config          Getting connector configuration.
    create          Creating a new connector.
    delete          Deleting a connector.
    delete-all      Deleting all connectors.
    pause           Pausing a connector (useful if downtime is needed for the system the connector interacts with).
    plugins         Listing installed connectors plugins.
    resume          Restarting a connector.
    restart-failed  Restarting failed tasks for a connector.
    status          Getting connector status.
    tasks           Getting tasks for a connector.
    scale           Scaling up the number of tasks for a connector.
    update          Updating connector configuration.
    version         Getting a connect worker version.

Use "kafka-connect-cli help [command]" for more information about that command.

```

### Examples

#### How to create a new connector instance ?

A new connector configuration can be passed through either json or properties file.

```
Usage of create: Creating a new connector.
The arguments are :
  -config string
    	The connector configuration json string. (Required)
  -config.json string
    	<file> The connector configuration json file. (Required)
  -config.props string
    	<file> The connector configuration properties file. (Required)
  -host string
    	The connector worker host address. (Required) (default "localhost")
  -port int
    	The connector worker port. (Required) (default 8083)
  -pretty
    	Pretty print json output.
```

#### How to display all connectors with failed tasks ?

Sometime it can be useful to quickly identify which connectors have failed tasks.

```bash
./kafka-connect-cli list -pretty -with-state failed
```

## Confluent Schema registry

A simple Command line interface (CLI) to manage [Confluent](http://docs.confluent.io/current/schema-registry/docs/api.html) Schema Registry.

### Run command
```bash
./bin/schema-registry-cli
```

### Usage

```
A simple Command line interface (CLI) to manage Confluent Schema Registry.

Usage of ./bin/schemaregistrycli: command [arguments]
The commands are :

	compatibility            Getting subject compatibility level for a subject.
	exist                    Checking if a schema has already been registered under the specified subject
	get                      Getting a specific version of the schema registered under this subject
	global-compatibility     Getting the global compatibility level.
	register                 Registering a new schema under the specified subject.
	set-compatibility        Setting a new compatibility level.
	subjects                 Getting the list of registered subjects.
	test                     Testing schemas for compatibility against specific versions of a subject’s schema.
	versions                 Getting a list of versions registered under the specified subject.

Use "schema-registry-cli help [command]" for more information about that command.
```

### Examples

#### How to retrieve only Avro schema from subject version ?

The option `schema` can be used to retieve only Avro schema of given subject without the version of the SchemaRegistry.

```bash
./bin/schema-registry-cli get -pretty -schema -subject mySubject-value
```

## Contributions
Any contribution is welcome

## Licence
Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License
