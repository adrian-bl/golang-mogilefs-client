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

/*
Package mogilefs implements a mogilefs client library.

Example:
	mc := mogilefs.New(domain, trackers)
	mc.Create("new-key", "custom-class", os.Stdin);
*/
package mogilefs

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// MogileFsClient structure returned by New()
type MogileFsClient struct {
	// The domain used by this instance
	domain string
	// A list of trackers we should try to connect
	trackers []string
	// A list of known broken trackers
	dead_trackers map[string]time.Time
	// The last tracker used by us - may be an empty string
	last_tracker string
	// Generic timeout for dial
	dial_timeout time.Duration
}

// Optional argument to the GetPaths function
type GetPathsOpts struct {
	// Only return the tracker response - do not verify that the file actually exists
	NoVerify bool
	// The number of paths to return. Defaults to 2 (the minimum)
	Pathcount int
}

// Returns a new MogileFsClient.
func New(domain string, trackers []string) *MogileFsClient {
	return &MogileFsClient{
		domain:        domain,
		trackers:      trackers,
		dial_timeout:  time.Duration(1) * time.Second,
		dead_trackers: make(map[string]time.Time),
	}
}

// Returns the last tracker used (or better: 'touched') by the client (may return an empty string)
func (m *MogileFsClient) LastTracketr() string {
	return m.last_tracker
}

// Returns all known paths of the requested key.
//
// The upper limit of the returned paths may be adjusted by passing the optional
// GetPathsOpts argument to the function.
func (m *MogileFsClient) GetPaths(key string, opts *GetPathsOpts) (paths []string, err error) {
	// Set some sane defaults if caller didn't care
	if opts == nil {
		opts = &GetPathsOpts{NoVerify: true}
	}

	// returning two paths is the minimum, anything below doesn't make sense
	if opts.Pathcount < 2 {
		opts.Pathcount = 2
	}

	args := make(url.Values)
	args.Add("key", key)
	args.Add("domain", m.domain)
	args.Add("pathcount", fmt.Sprintf("%d", opts.Pathcount))
	args.Add("noverify", fmt.Sprintf("%d", boolToInt(opts.NoVerify)))

	values, rqerr := m.DoRequest(cmd_getpaths, args)
	err = rqerr

	if err == nil && values != nil {
		for i := 1; i < 255; i++ {
			thisPath := values.Get(fmt.Sprintf("path%d", i))
			if len(thisPath) == 0 {
				break
			} else {
				paths = append(paths, thisPath)
			}
		}

	}

	return
}

// Renames an existing key
func (m *MogileFsClient) Rename(oldname string, newname string) (err error) {
	args := make(url.Values)
	args.Add("domain", m.domain)
	args.Add("from_key", oldname)
	args.Add("to_key", newname)

	_, err = m.DoRequest(cmd_rename, args)
	return
}

// Deletes an existing key
func (m *MogileFsClient) Delete(key string) (err error) {
	args := make(url.Values)
	args.Add("domain", m.domain)
	args.Add("key", key)

	_, err = m.DoRequest(cmd_delete, args)
	return
}

// Returns debugging information about a key.
//
// This function should not be used to lookup paths - use GetPaths to do so.
func (m *MogileFsClient) Debug(key string) (values url.Values, err error) {
	args := make(url.Values)
	args.Add("domain", m.domain)
	args.Add("key", key)

	values, err = m.DoRequest(cmd_debug, args)
	return
}

// Returns an io.ReadCloser with the contents of the requested key.
func (m *MogileFsClient) Fetch(key string) (r io.ReadCloser, err error) {
	paths, perr := m.GetPaths(key, nil)
	err = perr

	if err == nil {
		for _, path := range paths {
			rqResp, rqErr := http.Get(path)
			err = rqErr
			if err == nil {
				if rqResp.StatusCode == 200 {
					r = rqResp.Body
					break
				} else {
					err = fmt.Errorf("Invalid HTTP Status code: %d", rqResp.StatusCode)
				}
			}
		}
	}

	return
}

// Uploads (aka: sets) a new key in the filesystem.
//
// Note: Set 'class' to an empty string to use the default class of the filesystem.
func (m *MogileFsClient) Create(key string, class string, r io.Reader) (close_values url.Values, err error) {
	create_args := make(url.Values)
	create_args.Set("domain", m.domain)
	create_args.Set("key", key)
	create_args.Set("class", class)
	create_args.Set("fid", "0")
	create_args.Set("multi_dest", "0") // fixme: implement multi_dest ?

	create_values, err := m.DoRequest(cmd_create_open, create_args)
	cr := countingReader{r: r}

	if err == nil && len(create_values.Get("path")) > 0 {
		putRq, putErr := http.NewRequest("PUT", create_values.Get("path"), &cr)
		err = putErr

		if err == nil {
			client := &http.Client{}
			putRes, putErr := client.Do(putRq)
			err = putErr
			if err == nil {
				if putRes.StatusCode == 200 {
					close_args := make(url.Values)
					close_args.Set("domain", create_args.Get("domain"))
					close_args.Set("key", create_args.Get("key"))
					close_args.Set("fid", create_values.Get("fid"))
					close_args.Set("devid", create_values.Get("devid"))
					close_args.Set("path", create_values.Get("path"))
					close_args.Set("size", fmt.Sprintf("%d", cr.nbytes))
					close_values, err = m.DoRequest(cmd_create_close, close_args)
				} else {
					err = fmt.Errorf("Invalid HTTP Status code of storage daemon: %d", putRes.StatusCode)
				}
			}
		}

	}
	return
}

func boolToInt(value bool) (rv int) {
	if value {
		rv = 1
	}
	return
}
