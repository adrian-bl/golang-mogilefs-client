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

package mogilefs

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

/**
 * Structure of the client object
 */
type MogileFsClient struct {
	domain       string   // the domain used by this instance
	trackers     []string // a list of trackers we should try to connect
	last_tracker string   // the last tracker used by us - may be an empty string
	dial_timeout time.Duration
}

/**
 * Structure of opts for GetPaths
 */
type GetPathsOpts struct {
	NoVerify  bool // only return the tracker response -> do not verify that the file exists
	Pathcount int  // the number of paths to return, defaults to 2 (the minimum)
}

/**
 * @desc Constructs a new mogilefs client object
 * @param domain string the domain to use for this client
 * @param trackers []string list trackers to use. The passed string is expected to be parseable by golangs dial function
 * @return MogileFsClient struct
 */
func New(domain string, trackers []string) *MogileFsClient {
	return &MogileFsClient{
		domain:       domain,
		trackers:     trackers,
		dial_timeout: time.Duration(1) * time.Second,
	}
}

/**
 * @desc Returns the last used tracker
 * @return string of the last tracker host, may be an empty string
 */
func (m *MogileFsClient) LastTracketr() string {
	return m.last_tracker
}

/**
 * Returns a list of available paths for given key
 * @param key string the key to lookup
 * @param opts *GetPathsOpts optional parameters, may be nil - see 'GetPathsOpts struct'
 * @return paths []string list of available paths - may be empty on MISS
 * @return err error due to connection or tracker timeout issues
 */
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

	values, rqerr := m.DoRequest(CMD_GETPATHS, args)
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

/**
* Renames an existing key
" @param oldname string name of the key to rename
* @param newname string the new name to use for this key
* @return err error message from tracker, nil on success
*/
func (m *MogileFsClient) Rename(oldname string, newname string) (err error) {
	args := make(url.Values)
	args.Add("domain", m.domain)
	args.Add("from_key", oldname)
	args.Add("to_key", newname)

	_, err = m.DoRequest(CMD_RENAME, args)
	return
}

/**
 * Removes given key from the configured mogilefs domain
 * @param key string the key to remove
 * @return err error message from tracker, nil on success
 */
func (m *MogileFsClient) Delete(key string) (err error) {
	args := make(url.Values)
	args.Add("domain", m.domain)
	args.Add("key", key)

	_, err = m.DoRequest(CMD_DELETE, args)
	return
}

/**
 * Returns debuggin information about a key
 * @param key string the key to debug
 * @return err error message from tracker, nil on success
 */
func (m *MogileFsClient) Debug(key string) (values url.Values, err error) {
	args := make(url.Values)
	args.Add("domain", m.domain)
	args.Add("key", key)

	values, err = m.DoRequest(CMD_DEBUG, args)
	return
}

/**
 * Attempts to fetch given key
 * @param key string the key to fetch
 * @return r io.ReadCloser from the http body response
 * @return err error - nil on success
 */
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

/**
 * Uploads (aka: sets) a new key to the filesystem
 * @param key string the key to create
 * @param class string the class to use for this file. The default class equals an empty string
 * @param r io.Reader the reader to fetch the data from.
 * @return close_values url.Values The reply to CREATE_CLOSE
 * @return err error message of mogilefsd, nil on success
 */
func (m *MogileFsClient) Create(key string, class string, r io.Reader) (close_values url.Values, err error) {
	create_args := make(url.Values)
	create_args.Set("domain", m.domain)
	create_args.Set("key", key)
	create_args.Set("class", class)
	create_args.Set("fid", "0")
	create_args.Set("multi_dest", "0") // fixme: implement multi_dest ?

	create_values, err := m.DoRequest(CMD_CREATE_OPEN, create_args)
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
					close_values, err = m.DoRequest(CMD_CREATE_CLOSE, close_args)
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
