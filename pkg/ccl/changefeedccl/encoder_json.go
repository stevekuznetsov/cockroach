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
	gojson "encoding/json"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// jsonEncoder encodes changefeed entries as JSON. Keys are the primary key
// columns in a JSON array. Values are a JSON object mapping every column name
// to its value. Updated timestamps in rows and resolved timestamp payloads are
// stored in a sub-object under the `__crdb__` key in the top-level JSON object.
type jsonEncoder struct {
	*datumEncoder
}

func (e *jsonEncoder) EncodeKey(_ context.Context, row encodeRow) ([]byte, error) {
	entries, err := e.datumsForKey(row)
	if err != nil {
		return nil, err
	}
	if entries == nil {
		return nil, nil
	}
	jsonEntries, err := labelledDatumsToEntries(*entries)
	if err != nil {
		return nil, err
	}
	j, err := json.MakeJSON(jsonEntries)
	if err != nil {
		return nil, err
	}
	e.buf.Reset()
	j.Format(&e.buf)
	return e.buf.Bytes(), nil
}

func (e *jsonEncoder) EncodeValue(ctx context.Context, row encodeRow) ([]byte, error) {
	filtered, err := e.filterRow(ctx, row)
	if err != nil {
		return nil, err
	}
	if filtered == nil {
		return nil, nil
	}

	var after map[string]interface{}
	if filtered.after != nil {
		var err error
		after, err = labelledDatumsToJSON(*filtered.after)
		if err != nil {
			return nil, err
		}
	}

	var before map[string]interface{}
	if filtered.before != nil {
		var err error
		before, err = labelledDatumsToJSON(*filtered.before)
		if err != nil {
			return nil, err
		}
	}

	var jsonEntries map[string]interface{}
	if e.wrapped {
		if after != nil {
			jsonEntries = map[string]interface{}{`after`: after}
		} else {
			jsonEntries = map[string]interface{}{`after`: nil}
		}
		if before != nil {
			if len(before) > 0 {
				jsonEntries[`before`] = before
			} else {
				jsonEntries[`before`] = nil
			}
		}
		if filtered.key != nil {
			keyEntries, err := labelledDatumsToEntries(*filtered.key)
			if err != nil {
				return nil, err
			}
			jsonEntries[`key`] = keyEntries
		}
		if filtered.topic != nil {
			jsonEntries[`topic`] = *filtered.topic
		}
	} else {
		jsonEntries = after
	}

	if filtered.updated != nil || filtered.mvccTimestamp != nil {
		var meta map[string]interface{}
		if e.wrapped {
			meta = jsonEntries
		} else {
			meta = make(map[string]interface{}, 1)
			jsonEntries[jsonMetaSentinel] = meta
		}
		if filtered.updated != nil {
			meta[`updated`] = row.updated.AsOfSystemTime()
		}
		if filtered.mvccTimestamp != nil {
			meta[`mvcc_timestamp`] = row.mvccTimestamp.AsOfSystemTime()
		}
	}

	j, err := json.MakeJSON(jsonEntries)
	if err != nil {
		return nil, err
	}
	e.buf.Reset()
	j.Format(&e.buf)
	return e.buf.Bytes(), nil
}

func (e *jsonEncoder) EncodeResolvedTimestamp(
	_ context.Context, _ string, resolved hlc.Timestamp,
) ([]byte, error) {
	meta := map[string]interface{}{
		`resolved`: eval.TimestampToDecimalDatum(resolved).Decimal.String(),
	}
	var jsonEntries interface{}
	if e.wrapped {
		jsonEntries = meta
	} else {
		jsonEntries = map[string]interface{}{
			jsonMetaSentinel: meta,
		}
	}
	return gojson.Marshal(jsonEntries)
}

var _ Encoder = &jsonEncoder{}

func makeJSONEncoder(
	opts map[string]string, targets []jobspb.ChangefeedTargetSpecification,
) (*jsonEncoder, error) {
	e, err := makeDatumEncoder(opts, targets)
	if err != nil {
		return nil, err
	}
	return &jsonEncoder{datumEncoder: e}, nil
}


func labelledDatumsToEntries(datums []labelledDatum) ([]interface{}, error) {
	keyEntries := make([]interface{}, len(datums))
	for i, item := range datums {
		var err error
		keyEntries[i], err = tree.AsJSON(
			item.datum,
			sessiondatapb.DataConversionConfig{},
			time.UTC,
		)
		if err != nil {
			return nil, err
		}
	}
	return keyEntries, nil
}

func labelledDatumsToJSON(datums []labelledDatum) (map[string]interface{}, error) {
	output := make(map[string]interface{}, len(datums))
	for _, item := range datums {
		var err error
		output[item.columnName], err = tree.AsJSON(
			item.datum,
			sessiondatapb.DataConversionConfig{},
			time.UTC,
		)
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}
