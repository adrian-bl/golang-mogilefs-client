/*

Copyright 2015 Adrian Ulrich
Copyright 2015 Fixxpunkt AG

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
	"time"
)

const (
	blacklist_duration = time.Duration(60) * time.Second
)

/**
 * Checks if given tracker is known to be misbehaving
 * @param tracker string host string of the tracker to check
 * @param isdown bool true if the tracker should be avoided
 */
func (m *MogileFsClient) trackerIsBad(tracker string) (isdown bool) {

	if m.dead_trackers[tracker].IsZero() == false {
		// tracker is blacklisted, check if the blacklist is still active
		if m.dead_trackers[tracker].Before(time.Now()) == true {
			m.markTrackerAsAlive(tracker)
		} else {
			isdown = true
		}
	}
	return
}

/**
 * Adds a tracker to the blacklist
 * @param tracker string host string of the tracker to blacklist
 */
func (m *MogileFsClient) markTrackerAsBad(tracker string) {
	if m.trackerIsBad(tracker) == false {
		// -> not known to be bad: add it to blacklist
		m.dead_trackers[tracker] = time.Now().Add(blacklist_duration)
	}
}

/**
 * Forcefully removes a tracker from the blacklist
 * @param tracker string host string of the tracker to check
 */
func (m *MogileFsClient) markTrackerAsAlive(tracker string) {
	if m.dead_trackers[tracker].IsZero() == false {
		delete(m.dead_trackers, tracker)
	}
}
