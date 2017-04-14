Kafka CLI
=============================================================

[Apache Kafka](http://kafka.apache.org/) is a high-throughput, distributed, publish-subscribe messaging system.

## Kafka Connect CLI

A simple Command line interface (CLI) to manage connectors through the Kafka Connect REST Interface.

### Usage

```bash
./kafkaconnectcli

A simple Command line interface (CLI) to manage connectors through the Kafka Connect REST Interface.

Usage of ./bin/kafkaconnectcli: command [arguments] 
The commands are : 

	list		Listing active connectors on a worker.
	config		Getting connector configuration.
	create		Creating a new connector.
	delete		Deleting a connector.
	delete-all	Deleting all connectors.
	pause		Pausing a connector (useful if downtime is needed for the system the connector interacts with).
	plugins		Listing installed connectors plugins.
	resume		Restarting a connector.
	restart-failed	Restarting failed tasks for a connector.
	status		Getting connector status.
	tasks		Getting tasks for a connector.
	scale		Scaling up the number of tasks for a connector.
	update		Updating connector configuration.
	version		Getting a connect worker version.

Use "kafkaconnectcli help [command]" for more information about that command.

```

## Contributions
Any contribution is welcome

## Licence
Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License