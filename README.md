golang-mogilefs-client
========================

A client library to interact with a [mogilefs](https://github.com/mogilefs/) installation using golang.


Download the sourcecode
========================

The code is 'go gettable' and can be fetched via:

```
$ go get github.com/adrian-bl/golang-mogilefs-client/mogilefs
```

This will automatically fetch and install the library into your $GOPATH directory.


Building a simple client
========================

A very simple client may look like this:

```
package main

import (
	"fmt"
	"github.com/adrian-bl/golang-mogilefs-client/mogilefs"
)

func main() {

	trackerlist := make([]string, 1)
	trackerlist = append(trackerlist, "localhost:7001")
	trackerlist = append(trackerlist, "192.168.1.1:7001")

	mc := mogilefs.New("example.com", trackerlist)

	values, err := mc.Debug("example-key")
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("%v\n", values)
	}
}
```

A somewhat more advanced client can be found in the [cmd/demo](https://github.com/adrian-bl/golang-mogilefs-client/tree/master/cmd/demo) directory.


Documentation
========================
The package includes (documentation in godoc format)(http://godoc.org/github.com/adrian-bl/golang-mogilefs-client/mogilefs).
