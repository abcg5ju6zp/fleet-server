// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package bulk

import "time"

type queueT struct {
	ty      queueType
	cnt     int
	head    *bulkT
	pending int
	// controlCnt counts queued control blocks; when zero and pending>0
	// the queue is background-only and may be deferred under overload.
	controlCnt int
	// bgDeferredAt is set when a background-only queue is first deferred
	// for lack of a flush permit; it bounds starvation.
	bgDeferredAt time.Time
}

type queueType int

const (
	kQueueBulk queueType = iota
	kQueueRead
	kQueueSearch
	kQueueFleetSearch
	kQueueEnrollSearch
	kQueueRefreshBulk
	kQueueRefreshRead
	kQueueAPIKeyUpdate
	kNumQueues
)

func (q queueT) Type() string {
	switch q.ty {
	case kQueueBulk:
		return "bulk"
	case kQueueRead:
		return "read"
	case kQueueSearch:
		return "search"
	case kQueueFleetSearch:
		return "fleetSearch"
	case kQueueEnrollSearch:
		return "enrollSearch"
	case kQueueRefreshBulk:
		return "refreshBulk"
	case kQueueRefreshRead:
		return "refreshRead"
	case kQueueAPIKeyUpdate:
		return "apiKeyUpdate"
	}
	panic("unknown")
}
