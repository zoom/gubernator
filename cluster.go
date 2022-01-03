package gubernator

import (
	"github.com/mailgun/holster/v4/syncutil"
	"github.com/sirupsen/logrus"
)

type remoteClusterManager struct {
	reqQueue chan *RateLimitReq
	wg       syncutil.WaitGroup
	conf     BehaviorConfig
	log      logrus.FieldLogger
	instance *V1Instance
}

func newRemoteClusterManager(conf BehaviorConfig, instance *V1Instance) *remoteClusterManager {
	mm := remoteClusterManager{
		conf:     conf,
		instance: instance,
		log:      instance.log,
		reqQueue: make(chan *RateLimitReq, conf.MultiClusterBatchLimit),
	}
	mm.runAsyncReqs()
	return &mm
}

// QueueHits writes the RateLimitReq to be asynchronously sent to other clusters
func (mm *remoteClusterManager) QueueHits(r *RateLimitReq) {
	mm.reqQueue <- r
}

func (mm *remoteClusterManager) runAsyncReqs() {
	var interval = NewInterval(mm.conf.MultiClusterSyncWait)
	hits := make(map[string]*RateLimitReq)

	mm.wg.Until(func(done chan struct{}) bool {
		select {
		case r := <-mm.reqQueue:
			key := r.HashKey()

			// Aggregate the hits into a single request
			_, ok := hits[key]
			if ok {
				hits[key].Hits += r.Hits
			} else {
				hits[key] = r
			}

			// Send the hits if we reached our batch limit
			if len(hits) == mm.conf.MultiClusterBatchLimit {
				for dc, picker := range mm.instance.GetClusterPickers() {
					mm.log.Debugf("Sending %v hit(s) to %s picker", len(hits), dc)
					mm.sendHits(hits, picker)
				}
				hits = make(map[string]*RateLimitReq)
			}

			// Queue next interval
			if len(hits) == 1 {
				interval.Next()
			}

		case <-interval.C:
			if len(hits) > 0 {
				for dc, picker := range mm.instance.GetClusterPickers() {
					mm.log.Debugf("Sending %v hit(s) to %s picker", len(hits), dc)
					mm.sendHits(hits, picker)
				}
				hits = make(map[string]*RateLimitReq)
			}

		case <-done:
			return false
		}
		return true
	})
}

// TODO: Setup the cluster pickers based on the cluster config set by the admin.
// TODO: Need to consider how SetPeer() behaves... as it currently over writes
//  all current peer information, so once the local cluster changes we will loose
//  the static config provided by the admin.

func (mm *remoteClusterManager) sendHits(r map[string]*RateLimitReq, picker PeerPicker) {
	// TODO: Set ALWAYS_COUNT_HITS behavior so our hits are not ignored by the remote peer if the RL is
	//  over the limit.

	// TODO: If the picker has more than one entry we might assume that we are NOT sending rate limits to a DNS and thus
	//  the remote instance will NOT need to look up the owning instances as the local instance of the picker
	//  should pick the correct remote instance. In this case we set HASH_COMPUTED behavior on the rate limits we
	//  send over. (Might consider adding an special flag in the cluster that indicates this is the case)

	// TODO: Send the hit to the remote peer
}

func (mm *remoteClusterManager) Close() {
	mm.wg.Stop()
}
