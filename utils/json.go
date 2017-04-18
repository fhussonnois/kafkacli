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
	"encoding/json"
	"fmt"
	"strings"
)

// PrintJson prints a struct, string, array or map as JSON string.
func PrintJson(o interface{}, pretty bool) {
	var input interface{}

	switch o.(type) {
	case string:
		s := o.(string)
		if !pretty {
			fmt.Println(s)
			return
		}

		if isArray(s) {
			var m []map[string]interface{}
			if err := json.Unmarshal([]byte(s), &m); err != nil {
				panic(err)
			}
			input = m
		} else {
			var m map[string]interface{}
			if err := json.Unmarshal([]byte(s), &m); err != nil {
				panic(err)
			}
			input = m
		}
	default:
		input = o
	}

	if pretty {
		res, _ := json.MarshalIndent(input, "", "    ")
		fmt.Println(string(res))
	} else {
		res, _ := json.Marshal(input)
		fmt.Println(string(res))
	}
}

func isArray(s string) bool {
	return strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]")
}
