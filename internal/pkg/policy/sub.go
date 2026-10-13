// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package policy

import (
	"github.com/elastic/fleet-server/v7/internal/pkg/model"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

type subT struct {
	policyID string
	agentID  string // not logically necessary; cached for logging
	revIdx   int64
	owner    tenant.Owner

	next *subT
	prev *subT

	ch chan *ParsedPolicy
}

func NewSub(policyID, agentID string, revIdx int64, owner tenant.Owner) *subT {
	return &subT{
		policyID: policyID,
		agentID:  agentID,
		revIdx:   revIdx,
		owner:    owner,
		ch:       make(chan *ParsedPolicy, 1),
	}
}

func makeHead() *subT {
	sub := &subT{}
	sub.next = sub
	sub.prev = sub
	return sub
}

func (n *subT) pushFront(nn *subT) {
	nn.next = n.next
	nn.prev = n
	n.next.prev = nn
	n.next = nn
}

func (n *subT) pushBack(nn *subT) {
	nn.next = n
	nn.prev = n.prev
	n.prev.next = nn
	n.prev = nn
}

func (n *subT) popFront() *subT {
	if n.next == n {
		return nil
	}
	s := n.next
	s.unlink()
	return s
}

func (n *subT) unlink() bool { //nolint:unparam // useful to return this if we ever test
	if n.next == nil || n.prev == nil {
		return false
	}

	n.prev.next = n.next
	n.next.prev = n.prev
	n.next = nil
	n.prev = nil
	return true
}

func (n *subT) isEmpty() bool {
	return n.next == n
}

// popFair removes and returns the subscription with the oldest dispatch
// credit among the candidate class (cloud policies are served before
// ordinary ones, preserving historical priority). Owners never served
// have credit 0 and go first; ties keep FIFO. skip lists owners that
// must not be dispatched this round (e.g. namespace budget rejected).
func (n *subT) popFair(cloudPolicy map[string]bool, lastServed map[tenant.Owner]int64, skip map[tenant.Owner]struct{}) *subT {
	// One pass per priority class: cloud policies first when any are
	// waiting, then ordinary policies. Within a class the subscription
	// whose owner has the oldest dispatch credit wins; ties keep FIFO.
	for _, cloudWanted := range []bool{true, false} {
		var chosen *subT
		chosenCredit := int64(0)
		for cur := n.next; cur != n; cur = cur.next {
			if _, ignored := skip[cur.owner]; ignored {
				continue
			}
			if cloudPolicy[cur.policyID] != cloudWanted {
				continue
			}
			if chosen == nil || lastServed[cur.owner] < chosenCredit {
				chosen = cur
				chosenCredit = lastServed[cur.owner]
			}
		}
		if chosen != nil {
			chosen.unlink()
			return chosen
		}
	}
	return nil
}

func (n *subT) isUpdate(policy *model.Policy) bool {
	pRevIdx := policy.RevisionIdx

	return pRevIdx > n.revIdx
}

// Output returns a new policy that needs to be sent based on the current subscription.
func (n *subT) Output() <-chan *ParsedPolicy {
	return n.ch
}

type subIterT struct {
	head *subT
	cur  *subT
}

func NewIterator(head *subT) *subIterT {
	return &subIterT{
		head: head,
		cur:  head,
	}
}

func (it *subIterT) Next() *subT {
	next := it.cur.next
	if next == it.head {
		return nil
	}
	it.cur = next
	return next
}

func (it *subIterT) Unlink() {
	prev := it.cur.prev
	it.cur.unlink()
	it.cur = prev
}
