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
		reqQueue: make(chan *RateLimitReq, conf.MultiClusterBatchLimit),
		log:      instance.log,
		instance: instance,
		conf:     conf,
	}
	mm.runAsyncReqs()
	return &mm
}

// QueueHits writes the RateLimitReq to be asynchronously sent to other clusters
func (m *remoteClusterManager) QueueHits(r *RateLimitReq) {
	m.reqQueue <- r
}

func (m *remoteClusterManager) runAsyncReqs() {
	var interval = NewInterval(m.conf.MultiClusterSyncWait)
	hits := make(map[string]*RateLimitReq)

	m.wg.Until(func(done chan struct{}) bool {
		select {
		case r := <-m.reqQueue:
			key := r.HashKey()

			// Aggregate the hits into a single request
			_, ok := hits[key]
			if ok {
				hits[key].Hits += r.Hits
			} else {
				hits[key] = r
			}

			// Send the hits if we reached our batch limit
			if len(hits) == m.conf.MultiClusterBatchLimit {
				for dc, picker := range m.instance.GetClusterPickers() {
					m.log.Debugf("Sending %v hit(s) to %s picker", len(hits), dc)
					go m.sendHits(hits, picker)
				}
				hits = make(map[string]*RateLimitReq)
			}

			// Queue next interval
			if len(hits) == 1 {
				interval.Next()
			}

		case <-interval.C:
			if len(hits) > 0 {
				for dc, picker := range m.instance.GetClusterPickers() {
					m.log.Debugf("Sending %v hit(s) to %s picker", len(hits), dc)
					go m.sendHits(hits, picker)
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

func (m *remoteClusterManager) sendHits(r map[string]*RateLimitReq, picker PeerPicker) {
	type Batch struct {
		Peer       *PeerClient
		RateLimits []*RateLimitReq
	}

	// TODO: Set ALWAYS_COUNT_HITS behavior so our hits are not ignored by the remote peer if the RL is
	//  over the limit.

	if picker.Size() == 1 {
		// TODO: In this configuration no need to constantly call picker.Get() as we know all the
		//   RL are going to the same destination.
	}

	// TODO: If the picker has more than one entry we might assume that we are NOT sending rate limits to a DNS and thus
	//  the remote instance will NOT need to look up the owning instances as the local instance of the picker
	//  should pick the correct remote instance. In this case we set HASH_COMPUTED behavior on the rate limits we
	//  send over. (Might consider adding an special flag in the cluster that indicates this is the case)

	byPeer := make(map[string]*Batch)
	// For each key in the map, build a batch request to send to each peer
	for key, req := range r {
		p, err := picker.Get(key)
		if err != nil {
			m.log.WithError(err).Errorf("while asking remote picker for peer for key '%s'", key)
			continue
		}
		batch, ok := byPeer[p.Info().HashKey()]
		if ok {
			batch.RateLimits = append(batch.RateLimits, req)
		} else {
			byPeer[p.Info().HashKey()] = &Batch{Peer: p, RateLimits: []*RateLimitReq{req}}
		}
	}

	for _, batch := range byPeer {
		// TODO: Implement UpdateRateLimits()
		batch.Peer.UpdateRateLimits()
	}
	// TODO: Send the hits to the remote peer
}

func (m *remoteClusterManager) Close() {
	m.wg.Stop()
}
