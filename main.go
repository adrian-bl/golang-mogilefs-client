/*
Copyright 2015 Adrian Ulrich

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
	"mogilefs"
	"strings"
)

var flagDomain = flag.String("domain", "", "The domain to use for this request")
var flagTrackers = flag.String("trackers", "localhost:7001", "A list of trackers to use")
var flagInfoKey = flag.String("info", "", "The key to search and printout information about")
var flagRenameFrom = flag.String("rename_from", "", "RENAME: The key to rename")
var flagRenameTo = flag.String("rename_to", "", "RENAME: The new name of the key")
var flagDeleteKey = flag.String("delete", "", "The key to delete")

func main() {
	flag.Parse()

	trackerList := strings.Split(*flagTrackers, ",")

	if len(*flagInfoKey) != 0 {
		printKeyInfo(trackerList, *flagDomain, *flagInfoKey)
	} else if len(*flagDeleteKey) != 0 {
		deleteFile(trackerList, *flagDomain, *flagDeleteKey)
	} else if len(*flagRenameFrom) != 0 && len(*flagRenameTo) != 0 {
		renameFile(trackerList, *flagDomain, *flagRenameFrom, *flagRenameTo)
	} else {
		flag.PrintDefaults()
	}

}

func printKeyInfo(trackers []string, domain string, key string) {

	mc := mogilefs.New(domain, trackers)
	p, e := mc.GetPaths(key, &mogilefs.GetPathsOpts{NoVerify: true, Pathcount: 64})

	fmt.Printf("# details about '%s' on domain '%s' using %d tracker(s)\n", key, domain, len(trackers))
	fmt.Printf("copies = %d\n", len(p))

	for k, v := range p {
		fmt.Printf("path%d = %s\n", 1+k, v)
	}

	if e != nil {
		fmt.Printf("error = %s\n", e)
	}
}

func renameFile(trackers []string, domain, from string, to string) {
	mc := mogilefs.New(domain, trackers)
	e := mc.Rename(from, to)

	if e == nil {
		fmt.Printf("success\n")
	} else {
		fmt.Printf("error = %s\n", e)
	}
}

func deleteFile(trackers []string, domain, key string) {
	mc := mogilefs.New(domain, trackers)
	e := mc.Delete(key)

	if e == nil {
		fmt.Printf("success\n")
	} else {
		fmt.Printf("error = %s\n", e)
	}
}
