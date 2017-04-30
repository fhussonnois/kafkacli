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
package utils

import (
	"fmt"
	"os"
	"os/user"
	"strings"
)

// Resolve key from either environment variable or file ~/.kafkacli/hosts.
// Return the default value if no one can be resolved.
func GetUserLocalVarOrElse(key string, def string) string {
	val := os.Getenv(key)
	if val != "" {
		return val
	}
	usr, err := user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error while getting current user -  %s \n", err)
	}
	config, err := ReadProps(usr.HomeDir + "/.kafkacli/hosts")
	if err == nil {
		if key, ok := config[strings.ToLower(key)]; ok {
			return key
		}
	}
	return def
}
