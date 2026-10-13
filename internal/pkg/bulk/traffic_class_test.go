// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package bulk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/traffic"
)

var errClassProbe = errors.New("class probe stop")

func TestResolveClass(t *testing.T) {
	ctx := context.Background()

	assert.Equal(t, traffic.Control, resolveClass(ctx, optionsT{}),
		"class defaults to control when neither ctx nor opt carry one")

	bgCtx := traffic.WithClass(ctx, traffic.Background)
	assert.Equal(t, traffic.Background, resolveClass(bgCtx, optionsT{}),
		"class is inherited from context")

	opt := optionsT{}
	WithTrafficClass(traffic.Control)(&opt)
	assert.Equal(t, traffic.Control, resolveClass(bgCtx, opt),
		"explicit option overrides the context class")

	WithTrafficClass(traffic.Background)(&opt)
	assert.Equal(t, traffic.Background, resolveClass(ctx, opt))
}

func TestBlockCarriesAndResetsClass(t *testing.T) {
	b := NewBulker(nil, nil)

	opt := optionsT{trafficClassSet: true, trafficClass: traffic.Background}
	blk := b.newBlk(ActionSearch, opt)
	assert.Equal(t, traffic.Background, blk.class)

	blk.reset()
	assert.Equal(t, traffic.Control, blk.class, "reset restores the zero/default class")

	ctrl := b.newBlk(ActionCreate, optionsT{})
	assert.Equal(t, traffic.Control, ctrl.class)
}

// drainBlock reads one dispatched block without a Run loop, asserts its
// class and completes the operation with a probe error.
func drainBlock(t *testing.T, ch <-chan *bulkT, want traffic.Class) {
	t.Helper()
	select {
	case blk := <-ch:
		assert.Equal(t, want, blk.class)
		blk.ch <- respT{err: errClassProbe}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dispatched block")
	}
}

func TestDispatchedBlockInheritsContextClass(t *testing.T) {
	b := NewBulker(nil, nil)
	ctx := traffic.WithClass(context.Background(), traffic.Background)

	errCh := make(chan error, 1)
	go func() {
		_, err := b.Search(ctx, "idx", []byte(`{}`))
		errCh <- err
	}()

	drainBlock(t, b.ch, traffic.Background)
	require.ErrorIs(t, <-errCh, errClassProbe)
}

func TestMultiDispatchedBlocksInheritContextClass(t *testing.T) {
	b := NewBulker(nil, nil)
	ctx := traffic.WithClass(context.Background(), traffic.Background)
	ops := []MultiOp{
		{Index: "idx", ID: "a", Body: []byte(`{}`)},
		{Index: "idx", ID: "b", Body: []byte(`{}`)},
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := b.MUpdate(ctx, ops)
		errCh <- err
	}()

	drainBlock(t, b.ch, traffic.Background)
	drainBlock(t, b.ch, traffic.Background)
	require.ErrorIs(t, <-errCh, errClassProbe)
}
