package gubernator

import (
	"github.com/mailgun/holster/v4/syncutil"
)

type ClusterPeerPicker interface {
	GetClients(string) ([]*PeerClient, error)
	GetByPeerInfo(PeerInfo) *PeerClient
	Pickers() map[string]PeerPicker
	Peers() []*PeerClient
	Add(*PeerClient)
	New() ClusterPeerPicker
}

// ClusterPicker encapsulates pickers for a set of clusters
type ClusterPicker struct {
	*ReplicatedConsistentHash

	// A map of all the pickers by ClusterName
	clusters map[string]PeerPicker
	// The implementation of picker we will use for each cluster
	conf     BehaviorConfig
	wg       syncutil.WaitGroup
	reqQueue chan *RateLimitReq
}

func NewClusterPicker(fn HashString64) *ClusterPicker {
	rp := &ClusterPicker{
		ReplicatedConsistentHash: NewReplicatedConsistentHash(fn, defaultReplicas),
		clusters:                 make(map[string]PeerPicker),
		reqQueue:                 make(chan *RateLimitReq, 0),
	}
	return rp
}

func (rp *ClusterPicker) New() ClusterPeerPicker {
	hash := rp.ReplicatedConsistentHash.New().(*ReplicatedConsistentHash)
	return &ClusterPicker{
		clusters:                 make(map[string]PeerPicker),
		reqQueue:                 make(chan *RateLimitReq, 0),
		ReplicatedConsistentHash: hash,
	}
}

// GetClients returns all the PeerClients that match this key in all clusters
func (rp *ClusterPicker) GetClients(key string) ([]*PeerClient, error) {
	result := make([]*PeerClient, len(rp.clusters))
	var i int
	for _, picker := range rp.clusters {
		peer, err := picker.Get(key)
		if err != nil {
			return nil, err
		}
		result[i] = peer
		i++
	}
	return result, nil
}

// GetByPeerInfo returns the first PeerClient the PeerInfo.HasKey() matches
func (rp *ClusterPicker) GetByPeerInfo(info PeerInfo) *PeerClient {
	for _, picker := range rp.clusters {
		if client := picker.GetByPeerInfo(info); client != nil {
			return client
		}
	}
	return nil
}

// Pickers returns a map of each cluster and its respective PeerPicker
func (rp *ClusterPicker) Pickers() map[string]PeerPicker {
	return rp.clusters
}

func (rp *ClusterPicker) Peers() []*PeerClient {
	var peers []*PeerClient

	for _, picker := range rp.clusters {
		for _, peer := range picker.Peers() {
			peers = append(peers, peer)
		}
	}

	return peers
}

func (rp *ClusterPicker) Add(peer *PeerClient) {
	picker, ok := rp.clusters[peer.Info().ClusterName]
	if !ok {
		picker = rp.ReplicatedConsistentHash.New()
		rp.clusters[peer.Info().ClusterName] = picker
	}
	picker.Add(peer)
}
