// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// datumEncoder identifies the datums in a changefeed entry that should be emitted.
type datumEncoder struct {
	updatedField, mvccTimestampField, beforeField, wrapped, keyOnly, keyInValue, topicInValue bool

	targets                 []jobspb.ChangefeedTargetSpecification
	alloc                   tree.DatumAlloc
	buf                     bytes.Buffer
	virtualColumnVisibility string

	// columnMapCache caches the TableColMap for the latest version of the
	// table descriptor thus far seen. It avoids the need to recompute the
	// map per row, which, prior to the change introducing this cache, could
	// amount for 10% of row processing time.
	columnMapCache map[descpb.ID]*tableColumnMapCacheEntry
}

// tableColumnMapCacheEntry stores a TableColMap for a given descriptor version.
type tableColumnMapCacheEntry struct {
	version descpb.DescriptorVersion
	catalog.TableColMap
}

func makeDatumEncoder(
	opts map[string]string, targets []jobspb.ChangefeedTargetSpecification,
) (*datumEncoder, error) {
	e := &datumEncoder{
		targets:                 targets,
		keyOnly:                 changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) == changefeedbase.OptEnvelopeKeyOnly,
		wrapped:                 changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) == changefeedbase.OptEnvelopeWrapped,
		virtualColumnVisibility: opts[changefeedbase.OptVirtualColumns],
		columnMapCache:          map[descpb.ID]*tableColumnMapCacheEntry{},
	}
	_, e.updatedField = opts[changefeedbase.OptUpdatedTimestamps]
	_, e.mvccTimestampField = opts[changefeedbase.OptMVCCTimestamps]
	_, e.beforeField = opts[changefeedbase.OptDiff]
	if e.beforeField && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptDiff, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}
	_, e.keyInValue = opts[changefeedbase.OptKeyInValue]
	if e.keyInValue && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptKeyInValue, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}
	_, e.topicInValue = opts[changefeedbase.OptTopicInValue]
	if e.topicInValue && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptTopicInValue, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}
	return e, nil
}

func (e *datumEncoder) datumsForKey(row encodeRow) (*[]labelledDatum, error) {
	colIdxByID := e.getTableColMap(row.tableDesc)
	primaryIndex := row.tableDesc.GetPrimaryIndex()
	entries := make([]labelledDatum, primaryIndex.NumKeyColumns())
	for i := 0; i < primaryIndex.NumKeyColumns(); i++ {
		colID := primaryIndex.GetKeyColumnID(i)
		idx, ok := colIdxByID.Get(colID)
		if !ok {
			return nil, errors.Errorf(`unknown column id: %d`, colID)
		}
		datum, col := row.datums[idx], row.tableDesc.PublicColumns()[idx]
		if err := datum.EnsureDecoded(col.GetType(), &e.alloc); err != nil {
			return nil, err
		}
		entries[i] = labelledDatum{
			columnName: col.GetName(),
			datum:      datum.Datum,
		}
	}
	return &entries, nil
}

type filteredRow struct {
	before, after *[]labelledDatum

	key   *[]labelledDatum
	topic *string

	updated, mvccTimestamp *hlc.Timestamp
}

func (e *datumEncoder) filterRow(_ context.Context, row encodeRow) (*filteredRow, error) {
	if e.keyOnly || (!e.wrapped && row.deleted) {
		return nil, nil
	}

	var output filteredRow
	if !row.deleted {
		var err error
		output.after, err = e.datumsForRow(row.tableDesc, row.familyID, row.datums)
		if err != nil {
			return nil, err
		}
	}

	// there are three potential outputs here:
	// a - if the user did not request the field, no encoding
	// b - if the user requested the field but there was nothing there, null
	// c - if the user requested the field, and we had something, the values
	if e.beforeField {
		if row.prevDatums != nil && !row.prevDeleted {
			// case c
			var err error
			output.before, err = e.datumsForRow(row.prevTableDesc, row.prevFamilyID, row.prevDatums)
			if err != nil {
				return nil, err
			}
		} else {
			// case b
			nothing := make([]labelledDatum, 0) // *not* nil, to differentiate from case a
			output.before = &nothing
		}
	}
	// (implicit) case a

	if e.keyInValue { // TODO: we should always set this for the raw use-case, but that means we need to expose whether or not to include it
		var err error
		output.key, err = e.datumsForKey(row)
		if err != nil {
			return nil, err
		}
	}
	if e.topicInValue {
		output.topic = &row.topic
	}

	if e.updatedField {
		output.updated = &row.updated
	}
	if e.mvccTimestampField {
		output.mvccTimestamp = &row.mvccTimestamp
	}
	return &output, nil
}

type labelledDatum struct {
	columnName string
	datum      tree.Datum
}

func (e *datumEncoder) datumsForRow(tableDesc catalog.TableDescriptor, familyID descpb.FamilyID, datums rowenc.EncDatumRow) (*[]labelledDatum, error) {
	family, err := tableDesc.FindFamilyByID(familyID)
	if err != nil {
		return nil, err
	}
	include := make(map[descpb.ColumnID]struct{})
	var yes struct{}
	for _, colID := range family.ColumnIDs {
		include[colID] = yes
	}
	var before []labelledDatum
	for i, col := range tableDesc.PublicColumns() {
		_, inFamily := include[col.GetID()]
		virtual := col.IsVirtual() && e.virtualColumnVisibility == string(changefeedbase.OptVirtualColumnsNull)
		if inFamily || virtual {
			datum := datums[i]
			if err := datum.EnsureDecoded(col.GetType(), &e.alloc); err != nil {
				return nil, err
			}
			before = append(before, labelledDatum{
				columnName: col.GetName(),
				datum:      datum.Datum,
			})
		}
	}
	return &before, nil
}

// getTableColMap gets the TableColMap for the provided table descriptor,
// optionally consulting its cache.
func (e *datumEncoder) getTableColMap(desc catalog.TableDescriptor) catalog.TableColMap {
	ce, exists := e.columnMapCache[desc.GetID()]
	if exists {
		switch {
		case ce.version == desc.GetVersion():
			return ce.TableColMap
		case ce.version > desc.GetVersion():
			return catalog.ColumnIDToOrdinalMap(desc.PublicColumns())
		default:
			// Construct a new entry.
			delete(e.columnMapCache, desc.GetID())
		}
	}
	ce = &tableColumnMapCacheEntry{
		version:     desc.GetVersion(),
		TableColMap: catalog.ColumnIDToOrdinalMap(desc.PublicColumns()),
	}
	e.columnMapCache[desc.GetID()] = ce
	return ce.TableColMap
}
