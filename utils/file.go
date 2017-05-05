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
	"bufio"
	"log"
	"os"
	"strings"
)

// ReadProps reads simple properties file as map.
// Return a map containing all key-value pairs.
func ReadProps(filename string) (map[string]string, error) {
	config := make(map[string]string)
	if len(filename) == 0 {
		return config, nil
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// skip comments
		line = strings.TrimSpace(line)
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		pairs := strings.SplitN(line, "=", 2)
		value := ""
		key := pairs[0]
		if len(pairs) == 2 {
			value = pairs[1]
		}
		config[key] = value
	}
	// check for errors
	if err = scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return config, nil
}
