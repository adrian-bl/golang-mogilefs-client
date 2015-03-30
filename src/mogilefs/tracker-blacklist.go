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
