// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Emitter is an abstraction for anything that a changefeed may emit into.
type Emitter interface {
	// EmitRow enqueues a row message for asynchronous delivery on the sink. An
	// error may be returned if a previously enqueued message has failed.
	EmitRow(
		ctx context.Context,
		topic TopicDescriptor,
		row encodeRow,
		alloc kvevent.Alloc,
	) error
	// EmitResolvedTimestamp enqueues a resolved timestamp message for
	// asynchronous delivery on every topic that has been seen by EmitRow. An
	// error may be returned if a previously enqueued message has failed.
	EmitResolvedTimestamp(ctx context.Context, resolved hlc.Timestamp) error
	// Flush blocks until every message enqueued by EmitRow and
	// EmitResolvedTimestamp has been acknowledged by the sink. If an error is
	// returned, no guarantees are given about which messages have been
	// delivered or not delivered.
	Flush(ctx context.Context) error
	// Close does not guarantee delivery of outstanding messages.
	Close() error
}

func newEncoderSinkEmitter(encoder Encoder, sink Sink, knobs TestingKnobs) Emitter {
	return &encoderSinkEmitter{
		scratch: bufalloc.ByteAllocator{},
		encoder: encoder,
		sink:    &errorWrapperSink{wrapped: sink},
		knobs:   knobs,
	}
}

type encoderSinkEmitter struct {
	scratch bufalloc.ByteAllocator
	encoder Encoder
	sink    Sink
	knobs   TestingKnobs
}

func (e *encoderSinkEmitter) EmitRow(
	ctx context.Context, topic TopicDescriptor, row encodeRow, alloc kvevent.Alloc,
) error {
	var keyCopy, valueCopy []byte
	encodedKey, err := e.encoder.EncodeKey(ctx, row)
	if err != nil {
		return err
	}
	e.scratch, keyCopy = e.scratch.Copy(encodedKey, 0 /* extraCap */)
	encodedValue, err := e.encoder.EncodeValue(ctx, row)
	if err != nil {
		return err
	}
	e.scratch, valueCopy = e.scratch.Copy(encodedValue, 0 /* extraCap */)

	if e.knobs.BeforeEmitRow != nil {
		if err := e.knobs.BeforeEmitRow(ctx); err != nil {
			return err
		}
	}
	if err := e.sink.EmitRow(
		ctx, topic,
		keyCopy, valueCopy, row.updated, row.mvccTimestamp, alloc,
	); err != nil {
		return err
	}
	if log.V(3) {
		log.Infof(ctx, `r %s: %s -> %s`, row.tableDesc.GetName(), keyCopy, valueCopy)
	}
	return nil
}

func (e *encoderSinkEmitter) EmitResolvedTimestamp(
	ctx context.Context, resolved hlc.Timestamp,
) error {
	return e.sink.EmitResolvedTimestamp(ctx, e.encoder, resolved)
}

func (e *encoderSinkEmitter) Flush(ctx context.Context) error {
	return e.sink.Flush(ctx)
}

func (e *encoderSinkEmitter) Close() error {
	return e.sink.Close()
}

var _ Emitter = &encoderSinkEmitter{}
