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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type rawEmitter struct {
	enc datumEncoder
	buf     encDatumRowBuffer
	alloc   tree.DatumAlloc
	scratch bufalloc.ByteAllocator
	closed  bool
	metrics *sliMetrics
}

// EmitRow implements the Sink interface.
func (s *rawEmitter) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	row encodeRow,
	alloc kvevent.Alloc,
) error {
	defer alloc.Release(ctx)
	defer s.metrics.recordOneMessage()(row.mvccTimestamp, 0, sinkDoesNotCompress) // TODO: size?

	if s.closed {
		return errors.New(`cannot EmitRow on a closed sink`)
	}

	filtered, err := s.enc.filterRow(ctx, row)
	if err != nil {
		return err
	}

	encodedRow := rowenc.EncDatumRow{
		{Datum: tree.DNull}, // resolved span
		{Datum: s.getTopicDatum(topic)},
	}

	var keyTypes []*types.T
	var keyValues []tree.Datum
	var keyLabels []string
	for _, datum := range *filtered.key {
		keyTypes = append(keyTypes, datum.datum.ResolvedType())
		keyLabels = append(keyLabels, datum.columnName)
		keyValues = append(keyValues, datum.datum)
	}
	keyTupleType := types.MakeLabeledTuple(keyTypes, keyLabels)
	keyTuple := tree.MakeDTuple(keyTupleType, keyValues...)
	encodedRow = append(encodedRow, rowenc.EncDatum{Datum: s.alloc.NewDTuple(keyTuple)})

	if filtered.updated != nil {
		datum := eval.TimestampToDecimalDatum(*filtered.updated)
		encodedRow = append(encodedRow, rowenc.EncDatum{Datum: s.alloc.NewDDecimal(*datum)})
	}

	if filtered.mvccTimestamp != nil {
		datum := eval.TimestampToDecimalDatum(*filtered.mvccTimestamp)
		encodedRow = append(encodedRow, rowenc.EncDatum{Datum: s.alloc.NewDDecimal(*datum)})
	}

	s.buf.Push(rowenc.EncDatumRow{
		{Datum: tree.DNull}, // resolved span
		{Datum: s.getTopicDatum(topic)},
		{Datum: s.alloc.NewDBytes(tree.DBytes(key))},   // key
		{Datum: s.alloc.NewDBytes(tree.DBytes(value))}, // value
	})
	return nil
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *rawEmitter) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	if s.closed {
		return errors.New(`cannot EmitResolvedTimestamp on a closed sink`)
	}
	defer s.metrics.recordResolvedCallback()()

	var noTopic string
	payload, err := encoder.EncodeResolvedTimestamp(ctx, noTopic, resolved)
	if err != nil {
		return err
	}
	s.scratch, payload = s.scratch.Copy(payload, 0 /* extraCap */)
	s.buf.Push(rowenc.EncDatumRow{
		{Datum: tree.DNull}, // resolved span
		{Datum: tree.DNull}, // topic
		{Datum: tree.DNull}, // key
		{Datum: s.alloc.NewDBytes(tree.DBytes(payload))}, // value
	})
	return nil
}

// Flush implements the Sink interface.
func (s *rawEmitter) Flush(_ context.Context) error {
	defer s.metrics.recordFlushRequestCallback()()
	return nil
}

// Close implements the Sink interface.
func (s *rawEmitter) Close() error {
	s.closed = true
	return nil
}

// Dial implements the Sink interface.
func (s *rawEmitter) Dial() error {
	return nil
}

// TODO (zinger): Make this a tuple or array datum if it can be
// done without breaking backwards compatibility.
func (s *rawEmitter) getTopicDatum(t TopicDescriptor) *tree.DString {
	return s.alloc.NewDString(tree.DString(strings.Join(t.GetNameComponents(), ".")))
}

var _ Emitter = &rawEmitter{}
