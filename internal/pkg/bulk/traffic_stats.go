// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package bulk

// queueDepth is an immutable snapshot of one internal queue, published by
// the Run loop for metrics/debug endpoints. It never touches the hot path
// locks: Run stores a pointer at flush boundaries and readers load it
// atomically.
type queueDepth struct {
	Queue        string
	Items        int
	Bytes        int
	ControlItems int
}

type queueSnapshot struct {
	queues [kNumQueues]queueDepth
}

// QueueTrafficStat is the metrics-facing view of a queue depth.
type QueueTrafficStat struct {
	Queue        string
	Items        int
	Bytes        int
	ControlItems int
}

// BulkTrafficStats is the observability snapshot of bulk scheduling state.
type BulkTrafficStats struct {
	Enabled                      bool
	LegacyQueueLen               int
	ControlQueueLen              int
	BackgroundQueueLen           int
	PendingBackgroundDispatches  int64
	RejectedBackgroundDispatches int64
	Queues                       []QueueTrafficStat
}

func (b *Bulker) publishQueueSnapshot(queues *[kNumQueues]queueT) {
	var snap queueSnapshot
	for i := range queues {
		snap.queues[i] = queueDepth{
			Queue:        queues[i].Type(),
			Items:        queues[i].cnt,
			Bytes:        queues[i].pending,
			ControlItems: queues[i].controlCnt,
		}
	}
	b.queueSnapshot.Store(&snap)
}

// TrafficStats returns the latest scheduling snapshot for metrics/debug.
func (b *Bulker) TrafficStats() BulkTrafficStats {
	st := BulkTrafficStats{
		Enabled:                      b.trafficEnabled(),
		LegacyQueueLen:               len(b.ch),
		ControlQueueLen:              len(b.chControl),
		BackgroundQueueLen:           len(b.chBackground),
		PendingBackgroundDispatches:  b.pendingBgDispatches.Load(),
		RejectedBackgroundDispatches: b.rejectedBgDispatches.Load(),
	}
	if snap := b.queueSnapshot.Load(); snap != nil {
		st.Queues = make([]QueueTrafficStat, 0, len(snap.queues))
		for i := range snap.queues {
			st.Queues = append(st.Queues, QueueTrafficStat{
				Queue:        snap.queues[i].Queue,
				Items:        snap.queues[i].Items,
				Bytes:        snap.queues[i].Bytes,
				ControlItems: snap.queues[i].ControlItems,
			})
		}
	}
	return st
}
