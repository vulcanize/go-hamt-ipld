package hamt

import (
	"bytes"
	"context"
	"sync"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

// ParallelDiff returns a set of changes that transform node 'prev' into node 'cur'. opts are applied to both prev and cur.
func ParallelDiff(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur cid.Cid, workers int64, opts ...Option) ([]*Change, error) {
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

	return doParallelDiffNode(ctx, prevHamt, curHamt, workers)
}

// ParallelDiff returns a set of changes that transform node 'prev' into node 'cur'. opts are applied to both prev and cur.
func ParallelDiffTrackedWithNodeSink(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur cid.Cid, workers int64, b *bytes.Buffer, sink cbg.CBORUnmarshaler, opts ...Option) ([]*TrackedChange, error) {
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

	return doParallelDiffNodeTrackedWithNodeSink(ctx, prevHamt, curHamt, workers, b, sink, []int{})
}

func doParallelDiffNode(ctx context.Context, pre, cur *Node, workers int64) ([]*Change, error) {
	bp := cur.Bitfield.BitLen()
	if pre.Bitfield.BitLen() > bp {
		bp = pre.Bitfield.BitLen()
	}

	initTasks := []*task{}
	for idx := bp; idx >= 0; idx-- {
		preBit := pre.Bitfield.Bit(idx)
		curBit := cur.Bitfield.Bit(idx)
		initTasks = append(initTasks, &task{
			idx:    idx,
			pre:    pre,
			preBit: preBit,
			cur:    cur,
			curBit: curBit,
		})
	}

	out := make(chan *Change, 2*workers)
	differ, ctx := newDiffScheduler(ctx, workers, initTasks...)
	differ.startWorkers(ctx, out)
	differ.startScheduler(ctx)

	var changes []*Change
	done := make(chan struct{})
	go func() {
		defer close(done)
		for change := range out {
			changes = append(changes, change)
		}
	}()

	err := differ.grp.Wait()
	close(out)
	<-done

	return changes, err
}

func doParallelDiffNodeTrackedWithNodeSink(ctx context.Context, pre, cur *Node, workers int64, b *bytes.Buffer, sink cbg.CBORUnmarshaler, trail []int) ([]*TrackedChange, error) {
	bp := cur.Bitfield.BitLen()
	if pre.Bitfield.BitLen() > bp {
		bp = pre.Bitfield.BitLen()
	}

	initTasks := []*trackedTask{}
	for idx := bp; idx >= 0; idx-- {
		preBit := pre.Bitfield.Bit(idx)
		curBit := cur.Bitfield.Bit(idx)
		initTasks = append(initTasks, &trackedTask{
			task: task{
				idx:    idx,
				pre:    pre,
				preBit: preBit,
				cur:    cur,
				curBit: curBit,
			},
			trail: trail,
		})
	}

	out := make(chan *TrackedChange, 2*workers)
	differ, ctx := newTrackedWithNodeSinkDiffScheduler(ctx, workers, initTasks...)
	differ.startWorkers(ctx, out)
	differ.startScheduler(ctx)

	var changes []*TrackedChange
	done := make(chan struct{})
	go func() {
		defer close(done)
		for change := range out {
			changes = append(changes, change)
		}
	}()

	err := differ.grp.Wait()
	close(out)
	<-done

	return changes, err
}

type task struct {
	idx int

	pre    *Node
	preBit uint

	cur    *Node
	curBit uint
}

type trackedTask struct {
	task
	trail []int
}

func newDiffScheduler(ctx context.Context, numWorkers int64, rootTasks ...*task) (*diffScheduler, context.Context) {
	grp, ctx := errgroup.WithContext(ctx)
	s := &diffScheduler{
		numWorkers: numWorkers,
		stack:      rootTasks,
		in:         make(chan *task, numWorkers),
		out:        make(chan *task, numWorkers),
		grp:        grp,
	}
	s.taskWg.Add(len(rootTasks))
	return s, ctx
}

func newTrackedWithNodeSinkDiffScheduler(ctx context.Context, numWorkers int64, rootTasks ...*trackedTask) (*trackedScheduler, context.Context) {
	grp, ctx := errgroup.WithContext(ctx)
	s := &trackedScheduler{
		numWorkers: numWorkers,
		stack:      rootTasks,
		in:         make(chan *trackedTask, numWorkers),
		out:        make(chan *trackedTask, numWorkers),
		grp:        grp,
	}
	s.taskWg.Add(len(rootTasks))
	return s, ctx
}

type trackedScheduler struct {
	// number of worker routine to spawn
	numWorkers int64
	// buffer holds tasks until they are processed
	stack []*trackedTask
	// inbound and outbound tasks
	in, out chan *trackedTask
	// tracks number of inflight tasks
	taskWg sync.WaitGroup
	// launches workers and collects errors if any occur
	grp *errgroup.Group
	// node sink
	sink cbg.CBORUnmarshaler
}

type diffScheduler struct {
	// number of worker routine to spawn
	numWorkers int64
	// buffer holds tasks until they are processed
	stack []*task
	// inbound and outbound tasks
	in, out chan *task
	// tracks number of inflight tasks
	taskWg sync.WaitGroup
	// launches workers and collects errors if any occur
	grp *errgroup.Group
}

func (s *diffScheduler) enqueueTask(task *task) {
	s.taskWg.Add(1)
	s.in <- task
}

func (t *trackedScheduler) enqueueTask(task *trackedTask) {
	t.taskWg.Add(1)
	t.in <- task
}

func (s *diffScheduler) startScheduler(ctx context.Context) {
	s.grp.Go(func() error {
		defer func() {
			close(s.out)
			// Because the workers may have exited early (due to the context being canceled).
			for range s.out {
				s.taskWg.Done()
			}
			// Because the workers may have enqueued additional tasks.
			for range s.in {
				s.taskWg.Done()
			}
			// now, the waitgroup should be at 0, and the goroutine that was _waiting_ on it should have exited.
		}()
		go func() {
			s.taskWg.Wait()
			close(s.in)
		}()
		for {
			if n := len(s.stack) - 1; n >= 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-s.in:
					if !ok {
						return nil
					}
					s.stack = append(s.stack, newJob)
				case s.out <- s.stack[n]:
					s.stack[n] = nil
					s.stack = s.stack[:n]
				}
			} else {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-s.in:
					if !ok {
						return nil
					}
					s.stack = append(s.stack, newJob)
				}
			}
		}
	})
}

func (t *trackedScheduler) startScheduler(ctx context.Context) {
	t.grp.Go(func() error {
		defer func() {
			close(t.out)
			// Because the workers may have exited early (due to the context being canceled).
			for range t.out {
				t.taskWg.Done()
			}
			// Because the workers may have enqueued additional taskt.
			for range t.in {
				t.taskWg.Done()
			}
			// now, the waitgroup should be at 0, and the goroutine that was _waiting_ on it should have exited.
		}()
		go func() {
			t.taskWg.Wait()
			close(t.in)
		}()
		for {
			if n := len(t.stack) - 1; n >= 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-t.in:
					if !ok {
						return nil
					}
					t.stack = append(t.stack, newJob)
				case t.out <- t.stack[n]:
					t.stack[n] = nil
					t.stack = t.stack[:n]
				}
			} else {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-t.in:
					if !ok {
						return nil
					}
					t.stack = append(t.stack, newJob)
				}
			}
		}
	})
}

func (s *diffScheduler) startWorkers(ctx context.Context, out chan *Change) {
	for i := int64(0); i < s.numWorkers; i++ {
		s.grp.Go(func() error {
			for task := range s.out {
				if err := s.work(ctx, task, out); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

func (t *trackedScheduler) startWorkers(ctx context.Context, out chan *TrackedChange) {
	for i := int64(0); i < t.numWorkers; i++ {
		t.grp.Go(func() error {
			for task := range t.out {
				if err := t.work(ctx, task, out); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

func (s *diffScheduler) work(ctx context.Context, todo *task, results chan *Change) error {
	defer s.taskWg.Done()
	idx := todo.idx
	preBit := todo.preBit
	pre := todo.pre
	curBit := todo.curBit
	cur := todo.cur

	switch {
	case preBit == 1 && curBit == 1:
		// index for pre and cur will be unique to each, calculate it here.
		prePointer := pre.getPointer(byte(pre.indexForBitPos(idx)))
		curPointer := cur.getPointer(byte(cur.indexForBitPos(idx)))
		switch {
		// both pointers are shards, recurse down the tree.
		case prePointer.isShard() && curPointer.isShard():
			if prePointer.Link == curPointer.Link {
				return nil
			}
			preChild, err := prePointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
			if err != nil {
				return err
			}
			curChild, err := curPointer.loadChild(ctx, cur.store, cur.bitWidth, cur.hash)
			if err != nil {
				return err
			}

			bp := curChild.Bitfield.BitLen()
			if preChild.Bitfield.BitLen() > bp {
				bp = preChild.Bitfield.BitLen()
			}
			for idx := bp; idx >= 0; idx-- {
				preBit := preChild.Bitfield.Bit(idx)
				curBit := curChild.Bitfield.Bit(idx)
				s.enqueueTask(&task{
					idx:    idx,
					pre:    preChild,
					preBit: preBit,
					cur:    curChild,
					curBit: curBit,
				})
			}

		// check if KV's from cur exists in any children of pre's child.
		case prePointer.isShard() && !curPointer.isShard():
			childKV, err := prePointer.loadChildKVs(ctx, pre.store, pre.bitWidth, pre.hash)
			if err != nil {
				return err
			}
			parallelDiffKVs(childKV, curPointer.KVs, results)

		// check if KV's from pre exists in any children of cur's child.
		case !prePointer.isShard() && curPointer.isShard():
			childKV, err := curPointer.loadChildKVs(ctx, cur.store, cur.bitWidth, cur.hash)
			if err != nil {
				return err
			}
			parallelDiffKVs(prePointer.KVs, childKV, results)

		// both contain KVs, compare.
		case !prePointer.isShard() && !curPointer.isShard():
			parallelDiffKVs(prePointer.KVs, curPointer.KVs, results)
		}
	case preBit == 1 && curBit == 0:
		// there exists a value in previous not found in current - it was removed
		pointer := pre.getPointer(byte(pre.indexForBitPos(idx)))

		if pointer.isShard() {
			child, err := pointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
			if err != nil {
				return err
			}
			err = parallelRemoveAll(ctx, child, results)
			if err != nil {
				return err
			}
		} else {
			for _, p := range pointer.KVs {
				results <- &Change{
					Type:   Remove,
					Key:    string(p.Key),
					Before: p.Value,
					After:  nil,
				}
			}
		}
	case preBit == 0 && curBit == 1:
		// there exists a value in current not found in previous - it was added
		pointer := cur.getPointer(byte(cur.indexForBitPos(idx)))

		if pointer.isShard() {
			child, err := pointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
			if err != nil {
				return err
			}
			err = parallelAddAll(ctx, child, results)
			if err != nil {
				return err
			}
		} else {
			for _, p := range pointer.KVs {
				results <- &Change{
					Type:   Add,
					Key:    string(p.Key),
					Before: nil,
					After:  p.Value,
				}
			}
		}
	}
	return nil
}

func (t *trackedScheduler) work(ctx context.Context, todo *trackedTask, results chan *TrackedChange) error {
	defer t.taskWg.Done()
	idx := todo.idx
	preBit := todo.preBit
	pre := todo.pre
	curBit := todo.curBit
	cur := todo.cur

	l := len(todo.trail)
	subTrail := make([]int, l, l+1)
	copy(subTrail, todo.trail)
	subTrail = append(subTrail, idx)
	switch {
	case preBit == 1 && curBit == 1:
		// index for pre and cur will be unique to each, calculate it here.
		prePointer := pre.getPointer(byte(pre.indexForBitPos(idx)))
		curPointer := cur.getPointer(byte(cur.indexForBitPos(idx)))
		switch {
		// both pointers are shards, recurse down the tree.
		case prePointer.isShard() && curPointer.isShard():
			if prePointer.Link == curPointer.Link {
				return nil
			}
			preChild, err := prePointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
			if err != nil {
				return err
			}
			curChild, err := curPointer.loadChild(ctx, cur.store, cur.bitWidth, cur.hash)
			if err != nil {
				return err
			}

			bp := curChild.Bitfield.BitLen()
			if preChild.Bitfield.BitLen() > bp {
				bp = preChild.Bitfield.BitLen()
			}
			for idx := bp; idx >= 0; idx-- {
				preBit := preChild.Bitfield.Bit(idx)
				curBit := curChild.Bitfield.Bit(idx)
				t.enqueueTask(&trackedTask{
					task: task{
						idx:    idx,
						pre:    preChild,
						preBit: preBit,
						cur:    curChild,
						curBit: curBit,
					},
					trail: subTrail,
				})
			}

		// check if KV's from cur exists in any children of pre's child.
		case prePointer.isShard() && !curPointer.isShard():
			childKV, err := prePointer.loadChildKVs(ctx, pre.store, pre.bitWidth, pre.hash)
			if err != nil {
				return err
			}
			parallelDiffKVsTracked(childKV, curPointer.KVs, subTrail, results)

		// check if KV's from pre exists in any children of cur's child.
		case !prePointer.isShard() && curPointer.isShard():
			childKV, err := curPointer.loadChildKVs(ctx, cur.store, cur.bitWidth, cur.hash)
			if err != nil {
				return err
			}
			parallelDiffKVsTracked(prePointer.KVs, childKV, subTrail, results)

		// both contain KVs, compare.
		case !prePointer.isShard() && !curPointer.isShard():
			parallelDiffKVsTracked(prePointer.KVs, curPointer.KVs, subTrail, results)
		}
	case preBit == 1 && curBit == 0:
		// there exists a value in previous not found in current - it was removed
		pointer := pre.getPointer(byte(pre.indexForBitPos(idx)))

		if pointer.isShard() {
			child, err := pointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
			if err != nil {
				return err
			}
			err = parallelRemoveAllTracked(ctx, child, subTrail, results)
			if err != nil {
				return err
			}
		} else {
			for _, p := range pointer.KVs {
				results <- &TrackedChange{
					Change: Change{
						Type:   Remove,
						Key:    string(p.Key),
						Before: p.Value,
						After:  nil,
					},
					Path: subTrail,
				}
			}
		}
	case preBit == 0 && curBit == 1:
		// there exists a value in current not found in previous - it was added
		pointer := cur.getPointer(byte(cur.indexForBitPos(idx)))

		if pointer.isShard() {
			child, err := pointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
			if err != nil {
				return err
			}
			err = parallelAddAllTracked(ctx, child, subTrail, results)
			if err != nil {
				return err
			}
		} else {
			for _, p := range pointer.KVs {
				results <- &TrackedChange{
					Change: Change{
						Type:   Add,
						Key:    string(p.Key),
						Before: nil,
						After:  p.Value,
					},
					Path: subTrail,
				}
			}
		}
	}
	return nil
}

func parallelDiffKVs(pre, cur []*KV, out chan *Change) {
	preMap := make(map[string]*cbg.Deferred, len(pre))
	curMap := make(map[string]*cbg.Deferred, len(cur))

	for _, kv := range pre {
		preMap[string(kv.Key)] = kv.Value
	}
	for _, kv := range cur {
		curMap[string(kv.Key)] = kv.Value
	}
	// find removed keys: keys in pre and not in cur
	for key, value := range preMap {
		if _, ok := curMap[key]; !ok {
			out <- &Change{
				Type:   Remove,
				Key:    key,
				Before: value,
				After:  nil,
			}
		}
	}
	// find added keys: keys in cur and not in pre
	// find modified values: keys in cur and pre with different values
	for key, curVal := range curMap {
		if preVal, ok := preMap[key]; !ok {
			out <- &Change{
				Type:   Add,
				Key:    key,
				Before: nil,
				After:  curVal,
			}
		} else {
			if !bytes.Equal(preVal.Raw, curVal.Raw) {
				out <- &Change{
					Type:   Modify,
					Key:    key,
					Before: preVal,
					After:  curVal,
				}
			}
		}
	}
}

func parallelDiffKVsTracked(pre, cur []*KV, trail []int, out chan *TrackedChange) {
	preMap := make(map[string]*cbg.Deferred, len(pre))
	curMap := make(map[string]*cbg.Deferred, len(cur))

	for _, kv := range pre {
		preMap[string(kv.Key)] = kv.Value
	}
	for _, kv := range cur {
		curMap[string(kv.Key)] = kv.Value
	}
	// find removed keys: keys in pre and not in cur
	for key, value := range preMap {
		if _, ok := curMap[key]; !ok {
			out <- &TrackedChange{
				Change: Change{
					Type:   Remove,
					Key:    key,
					Before: value,
					After:  nil,
				},
				Path: trail,
			}
		}
	}
	// find added keys: keys in cur and not in pre
	// find modified values: keys in cur and pre with different values
	for key, curVal := range curMap {
		if preVal, ok := preMap[key]; !ok {
			out <- &TrackedChange{
				Change: Change{
					Type:   Add,
					Key:    key,
					Before: nil,
					After:  curVal,
				},
				Path: trail,
			}
		} else {
			if !bytes.Equal(preVal.Raw, curVal.Raw) {
				out <- &TrackedChange{
					Change: Change{
						Type:   Modify,
						Key:    key,
						Before: preVal,
						After:  curVal,
					},
					Path: trail,
				}
			}
		}
	}
}

func parallelAddAll(ctx context.Context, node *Node, out chan *Change) error {
	return node.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		out <- &Change{
			Type:   Add,
			Key:    k,
			Before: nil,
			After:  val,
		}
		return nil
	})
}

func parallelAddAllTracked(ctx context.Context, node *Node, trail []int, out chan *TrackedChange) error {
	return node.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		out <- &TrackedChange{
			Change: Change{
				Type:   Add,
				Key:    k,
				Before: nil,
				After:  val,
			},
			Path: trail,
		}
		return nil
	})
}

func parallelRemoveAll(ctx context.Context, node *Node, out chan *Change) error {
	return node.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		out <- &Change{
			Type:   Remove,
			Key:    k,
			Before: val,
			After:  nil,
		}
		return nil
	})
}

func parallelRemoveAllTracked(ctx context.Context, node *Node, trail []int, out chan *TrackedChange) error {
	return node.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		out <- &TrackedChange{
			Change: Change{
				Type:   Remove,
				Key:    k,
				Before: val,
				After:  nil,
			},
			Path: trail,
		}
		return nil
	})
}
