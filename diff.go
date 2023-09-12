package hamt

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

// ChangeType denotes type of change in Change
type ChangeType int

// These constants define the changes that can be applied to a DAG.
const (
	Add ChangeType = iota
	Remove
	Modify
)

// Change represents a change to a DAG and contains a reference to the old and
// new CIDs.
type Change struct {
	Type   ChangeType
	Key    string
	Before *cbg.Deferred
	After  *cbg.Deferred
}

// TrackedChange associates the updated leaf node path for the given state change
type TrackedChange struct {
	Change
	Path []int
}

func (ch Change) String() string {
	b, _ := json.Marshal(ch)
	return string(b)
}

// Diff returns a set of changes that transform node 'prev' into node 'cur'. opts are applied to both prev and cur.
func Diff(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur cid.Cid, opts ...Option) ([]*Change, error) {
	if prev.Equals(cur) {
		return nil, nil
	}

	prevHamt, err := LoadNode(ctx, prevBs, prev, opts...)
	if err != nil {
		return nil, err
	}

	curHamt, err := LoadNode(ctx, curBs, cur, opts...)
	if err != nil {
		return nil, err
	}

	if curHamt.bitWidth != prevHamt.bitWidth {
		return nil, xerrors.Errorf("diffing HAMTs with differing bitWidths not supported (prev=%d, cur=%d)", prevHamt.bitWidth, curHamt.bitWidth)
	}
	return diffNode(ctx, prevHamt, curHamt, 0)
}

// DiffTrackedWithNodeSink returns a set of changes that transform node 'prev' into node 'cur'. opts are applied to both prev and cur.
// it associates selector suffixes with the emitted Change set and sinks all unique nodes encountered under the current CID to the provided CBORUnmarshaler
func DiffTrackedWithNodeSink(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur cid.Cid, b *bytes.Buffer, sink cbg.CBORUnmarshaler, opts ...Option) ([]*TrackedChange, error) {
	if prev.Equals(cur) {
		return nil, nil
	}

	prevHamt, err := LoadNode(ctx, prevBs, prev, opts...)
	if err != nil {
		return nil, err
	}

	curHamt, err := LoadNode(ctx, curBs, cur, opts...)
	if err != nil {
		return nil, err
	}

	if curHamt.bitWidth != prevHamt.bitWidth {
		return nil, xerrors.Errorf("diffing HAMTs with differing bitWidths not supported (prev=%d, cur=%d)", prevHamt.bitWidth, curHamt.bitWidth)
	}
	return diffNodeTrackedWithNodeSink(ctx, prevHamt, curHamt, 0, b, sink, []int{})
}

func diffNode(ctx context.Context, pre, cur *Node, depth int) ([]*Change, error) {
	// which Bitfield contains the most bits. We will start a loop from this index, calling Bitfield.Bit(idx)
	// on an out of range index will return zero.
	bp := cur.Bitfield.BitLen()
	if pre.Bitfield.BitLen() > bp {
		bp = pre.Bitfield.BitLen()
	}

	// the changes between cur and prev
	var changes []*Change

	// loop over each bit in the bitfields
	for idx := bp; idx >= 0; idx-- {
		preBit := pre.Bitfield.Bit(idx)
		curBit := cur.Bitfield.Bit(idx)

		if preBit == 1 && curBit == 1 {
			// index for pre and cur will be unique to each, calculate it here.
			prePointer := pre.getPointer(byte(pre.indexForBitPos(idx)))
			curPointer := cur.getPointer(byte(cur.indexForBitPos(idx)))

			// both pointers are shards, recurse down the tree.
			if prePointer.isShard() && curPointer.isShard() {
				if prePointer.Link == curPointer.Link {
					continue
				}
				preChild, err := prePointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
				if err != nil {
					return nil, err
				}
				curChild, err := curPointer.loadChild(ctx, cur.store, cur.bitWidth, cur.hash)
				if err != nil {
					return nil, err
				}

				change, err := diffNode(ctx, preChild, curChild, depth+1)
				if err != nil {
					return nil, err
				}
				changes = append(changes, change...)
			}

			// check if KV's from cur exists in any children of pre's child.
			if prePointer.isShard() && !curPointer.isShard() {
				childKV, err := prePointer.loadChildKVs(ctx, pre.store, pre.bitWidth, pre.hash)
				if err != nil {
					return nil, err
				}
				changes = append(changes, diffKVs(childKV, curPointer.KVs, idx)...)

			}

			// check if KV's from pre exists in any children of cur's child.
			if !prePointer.isShard() && curPointer.isShard() {
				childKV, err := curPointer.loadChildKVs(ctx, cur.store, cur.bitWidth, cur.hash)
				if err != nil {
					return nil, err
				}
				changes = append(changes, diffKVs(prePointer.KVs, childKV, idx)...)
			}

			// both contain KVs, compare.
			if !prePointer.isShard() && !curPointer.isShard() {
				changes = append(changes, diffKVs(prePointer.KVs, curPointer.KVs, idx)...)
			}
		} else if preBit == 1 && curBit == 0 {
			// there exists a value in previous not found in current - it was removed
			pointer := pre.getPointer(byte(pre.indexForBitPos(idx)))

			if pointer.isShard() {
				child, err := pointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
				if err != nil {
					return nil, err
				}
				rm, err := removeAll(ctx, child, idx)
				if err != nil {
					return nil, err
				}
				changes = append(changes, rm...)
			} else {
				for _, p := range pointer.KVs {
					changes = append(changes, &Change{
						Type:   Remove,
						Key:    string(p.Key),
						Before: p.Value,
						After:  nil,
					})
				}
			}
		} else if curBit == 1 && preBit == 0 {
			// there exists a value in current not found in previous - it was added
			pointer := cur.getPointer(byte(cur.indexForBitPos(idx)))

			if pointer.isShard() {
				child, err := pointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
				if err != nil {
					return nil, err
				}
				add, err := addAll(ctx, child, idx)
				if err != nil {
					return nil, err
				}
				changes = append(changes, add...)
			} else {
				for _, p := range pointer.KVs {
					changes = append(changes, &Change{
						Type:   Add,
						Key:    string(p.Key),
						Before: nil,
						After:  p.Value,
					})
				}
			}
		}
	}

	return changes, nil
}

func diffNodeTrackedWithNodeSink(ctx context.Context, pre, cur *Node, depth int, b *bytes.Buffer, sink cbg.CBORUnmarshaler, trail []int) ([]*TrackedChange, error) {
	// which Bitfield contains the most bits. We will start a loop from this index, calling Bitfield.Bit(idx)
	// on an out of range index will return zero.
	bp := cur.Bitfield.BitLen()
	if pre.Bitfield.BitLen() > bp {
		bp = pre.Bitfield.BitLen()
	}

	if sink != nil {
		if b == nil {
			b = bytes.NewBuffer(nil)
		}
		b.Reset()
		if err := cur.MarshalCBOR(b); err != nil {
			return nil, err
		}
		if err := sink.UnmarshalCBOR(b); err != nil {
			return nil, err
		}
	}

	// the changes between cur and prev
	var changes []*TrackedChange
	l := len(trail)
	// loop over each bit in the bitfields
	for idx := bp; idx >= 0; idx-- {
		preBit := pre.Bitfield.Bit(idx)
		curBit := cur.Bitfield.Bit(idx)

		subTrail := make([]int, l, l+1)
		copy(subTrail, trail)
		subTrail = append(subTrail, idx)

		if preBit == 1 && curBit == 1 {
			// index for pre and cur will be unique to each, calculate it here.
			prePointer := pre.getPointer(byte(pre.indexForBitPos(idx)))
			curPointer := cur.getPointer(byte(cur.indexForBitPos(idx)))

			// both pointers are shards, recurse down the tree.
			if prePointer.isShard() && curPointer.isShard() {
				if prePointer.Link == curPointer.Link {
					continue
				}
				preChild, err := prePointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
				if err != nil {
					return nil, err
				}
				curChild, err := curPointer.loadChild(ctx, cur.store, cur.bitWidth, cur.hash)
				if err != nil {
					return nil, err
				}

				change, err := diffNodeTrackedWithNodeSink(ctx, preChild, curChild, depth+1, b, sink, subTrail)
				if err != nil {
					return nil, err
				}
				changes = append(changes, change...)
			}

			// check if KV's from cur exists in any children of pre's child.
			if prePointer.isShard() && !curPointer.isShard() {
				childKV, err := prePointer.loadChildKVs(ctx, pre.store, pre.bitWidth, pre.hash)
				if err != nil {
					return nil, err
				}
				changes = append(changes, diffKVsTracked(childKV, curPointer.KVs, subTrail)...)

			}

			// check if KV's from pre exists in any children of cur's child.
			if !prePointer.isShard() && curPointer.isShard() {
				childKV, err := curPointer.loadChildKVs(ctx, cur.store, cur.bitWidth, cur.hash)
				if err != nil {
					return nil, err
				}
				changes = append(changes, diffKVsTracked(prePointer.KVs, childKV, subTrail)...)
			}

			// both contain KVs, compare.
			if !prePointer.isShard() && !curPointer.isShard() {
				changes = append(changes, diffKVsTracked(prePointer.KVs, curPointer.KVs, subTrail)...)
			}
		} else if preBit == 1 && curBit == 0 {
			// there exists a value in previous not found in current - it was removed
			pointer := pre.getPointer(byte(pre.indexForBitPos(idx)))

			if pointer.isShard() {
				child, err := pointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
				if err != nil {
					return nil, err
				}
				rm, err := removeAllTracked(ctx, child, subTrail)
				if err != nil {
					return nil, err
				}
				changes = append(changes, rm...)
			} else {
				for _, p := range pointer.KVs {
					changes = append(changes, &TrackedChange{
						Change: Change{
							Type:   Remove,
							Key:    string(p.Key),
							Before: p.Value,
							After:  nil,
						},
						Path: subTrail,
					})
				}
			}
		} else if curBit == 1 && preBit == 0 {
			// there exists a value in current not found in previous - it was added
			pointer := cur.getPointer(byte(cur.indexForBitPos(idx)))

			if pointer.isShard() {
				child, err := pointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
				if err != nil {
					return nil, err
				}
				add, err := addAllTrackWithNodeSink(ctx, child, b, sink, subTrail)
				if err != nil {
					return nil, err
				}
				changes = append(changes, add...)
			} else {
				for _, p := range pointer.KVs {
					changes = append(changes, &TrackedChange{
						Change: Change{
							Type:   Add,
							Key:    string(p.Key),
							Before: nil,
							After:  p.Value,
						},
						Path: subTrail,
					})
				}
			}
		}
	}

	return changes, nil
}

func diffKVs(pre, cur []*KV, idx int) []*Change {
	preMap := make(map[string]*cbg.Deferred, len(pre))
	curMap := make(map[string]*cbg.Deferred, len(cur))
	var changes []*Change

	for _, kv := range pre {
		preMap[string(kv.Key)] = kv.Value
	}
	for _, kv := range cur {
		curMap[string(kv.Key)] = kv.Value
	}
	// find removed keys: keys in pre and not in cur
	for key, value := range preMap {
		if _, ok := curMap[key]; !ok {
			changes = append(changes, &Change{
				Type:   Remove,
				Key:    key,
				Before: value,
				After:  nil,
			})
		}
	}
	// find added keys: keys in cur and not in pre
	// find modified values: keys in cur and pre with different values
	for key, curVal := range curMap {
		if preVal, ok := preMap[key]; !ok {
			changes = append(changes, &Change{
				Type:   Add,
				Key:    key,
				Before: nil,
				After:  curVal,
			})
		} else {
			if !bytes.Equal(preVal.Raw, curVal.Raw) {
				changes = append(changes, &Change{
					Type:   Modify,
					Key:    key,
					Before: preVal,
					After:  curVal,
				})
			}
		}
	}
	return changes
}

func diffKVsTracked(pre, cur []*KV, trail []int) []*TrackedChange {
	preMap := make(map[string]*cbg.Deferred, len(pre))
	curMap := make(map[string]*cbg.Deferred, len(cur))
	var changes []*TrackedChange

	for _, kv := range pre {
		preMap[string(kv.Key)] = kv.Value
	}
	for _, kv := range cur {
		curMap[string(kv.Key)] = kv.Value
	}
	// find removed keys: keys in pre and not in cur
	for key, value := range preMap {
		if _, ok := curMap[key]; !ok {
			changes = append(changes, &TrackedChange{
				Change: Change{
					Type:   Remove,
					Key:    key,
					Before: value,
					After:  nil,
				},
				Path: trail,
			})
		}
	}
	// find added keys: keys in cur and not in pre
	// find modified values: keys in cur and pre with different values
	for key, curVal := range curMap {
		if preVal, ok := preMap[key]; !ok {
			changes = append(changes, &TrackedChange{
				Change: Change{
					Type:   Add,
					Key:    key,
					Before: nil,
					After:  curVal,
				},
				Path: trail,
			})
		} else {
			if !bytes.Equal(preVal.Raw, curVal.Raw) {
				changes = append(changes, &TrackedChange{
					Change: Change{
						Type:   Modify,
						Key:    key,
						Before: preVal,
						After:  curVal,
					},
					Path: trail,
				})
			}
		}
	}
	return changes
}

func addAll(ctx context.Context, node *Node, idx int) ([]*Change, error) {
	var changes []*Change
	if err := node.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		changes = append(changes, &Change{
			Type:   Add,
			Key:    k,
			Before: nil,
			After:  val,
		})

		return nil
	}); err != nil {
		return nil, err
	}
	return changes, nil
}

func addAllTrackWithNodeSink(ctx context.Context, node *Node, b *bytes.Buffer, sink cbg.CBORUnmarshaler, trail []int) ([]*TrackedChange, error) {
	var changes []*TrackedChange
	if err := node.ForEachTrackedWithNodeSink(ctx, trail, b, sink, func(k string, val *cbg.Deferred, selectorSuffix []int) error {
		changes = append(changes, &TrackedChange{
			Change: Change{
				Type:   Add,
				Key:    k,
				Before: nil,
				After:  val,
			},
			Path: selectorSuffix,
		})

		return nil
	}); err != nil {
		return nil, err
	}
	return changes, nil
}

func removeAll(ctx context.Context, node *Node, idx int) ([]*Change, error) {
	var changes []*Change
	if err := node.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		changes = append(changes, &Change{
			Type:   Remove,
			Key:    k,
			Before: val,
			After:  nil,
		})

		return nil
	}); err != nil {
		return nil, err
	}
	return changes, nil
}

func removeAllTracked(ctx context.Context, node *Node, trail []int) ([]*TrackedChange, error) {
	var changes []*TrackedChange
	if err := node.ForEachTracked(ctx, trail, func(k string, val *cbg.Deferred, selectorSuffix []int) error {
		changes = append(changes, &TrackedChange{
			Change: Change{
				Type:   Remove,
				Key:    k,
				Before: val,
				After:  nil,
			},
			Path: selectorSuffix,
		})

		return nil
	}); err != nil {
		return nil, err
	}
	return changes, nil
}
