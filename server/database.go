//    Copyright 2018 Google Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package server

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/danchia/ddb/memtable"
	pb "github.com/danchia/ddb/proto"
	"github.com/danchia/ddb/sst"
	"github.com/danchia/ddb/wal"
	"github.com/golang/glog"
	"go.opencensus.io/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type database struct {
	descriptor *Descriptor
	opts       Options

	memtable  *memtable.Memtable
	imemtable *memtable.Memtable

	logWriter *wal.Writer

	blockCache *sst.Cache
	ssts       []*sst.Reader

	mu sync.RWMutex
}

func newDatabase(opts Options) *database {
	db := &database{
		opts: opts,
	}

	ensureDir(opts.DescriptorDir)
	ensureDir(opts.LogDir)
	ensureDir(opts.SstDir)

	// Attempt to load descriptor
	descriptor, err := LoadDescriptor(opts.DescriptorDir)
	if os.IsNotExist(err) {
		glog.Warningf("could not find descriptor, assuming new database creation")
		descriptor = NewDescriptor(opts.DescriptorDir)
	} else if err != nil {
		glog.Fatalf("could not read descriptor: %v", err)
	}
	db.descriptor = descriptor

	// Initialize caches
	if opts.BlockCacheSize > 0 {
		db.blockCache = sst.NewCache(opts.BlockCacheSize)
	}

	lastAppliedSeqNo := int64(0)
	for _, sstMeta := range descriptor.Current.SstMeta {
		if sstMeta.AppliedUntil > lastAppliedSeqNo {
			lastAppliedSeqNo = sstMeta.AppliedUntil
		}
		sstReader, err := sst.NewReader(filepath.Join(opts.SstDir, sstMeta.Filename), db.blockCache)
		if err != nil {
			glog.Fatalf("Error while opening SST: %v", err)
		}
		db.ssts = append(db.ssts, sstReader)
	}

	db.memtable = memtable.New(lastAppliedSeqNo)

	nextSeq, err := db.recoverLog(lastAppliedSeqNo)
	if err != nil {
		glog.Fatalf("Failed to recover log file: %v", err)
	}

	logOpts := wal.Options{Dirname: opts.LogDir, TargetSize: opts.TargetLogSize}
	logWriter, err := wal.NewWriter(nextSeq, logOpts)
	if err != nil {
		glog.Fatalf("Error creating WAL writer: %v", err)
	}
	db.logWriter = logWriter

	go db.compactor()

	return db
}

func (d *database) recoverLog(lastApplied int64) (nextSeq int64, err error) {
	sc, err := wal.NewScanner(d.opts.LogDir)
	if os.IsNotExist(err) {
		glog.Infof("no log files found")
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	n := int64(0)
	applied := int64(0)
	seqNo := lastApplied

	for sc.Scan() {
		r := sc.Record()
		n++

		if glog.V(4) {
			glog.V(4).Infof("Read wal record: %v", r)
		}

		if r.Sequence <= seqNo {
			// we've already seen this, skip
			continue
		}
		applied++

		seqNo = r.Sequence
		if r.Mutation == nil {
			continue
		}
		d.apply(r)
	}
	d.maybeTriggerFlush()

	glog.Infof("Scanned %d log entries, applied %d", n, applied)

	if seqNo == -1 {
		// TODO: it's possible that if we truncate the log and don't have any new mutations
		// we won't get a sequence number, even if we can recover it from the file metadata.
		glog.Fatalf("seqNo was not recovered")
	}
	return seqNo, sc.Err()
}

func ensureDir(dir string) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		glog.Fatalf("error while ensuring directory %v: %v", dir, err)
	}
}

func (d *database) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := validateValue(req.Value); err != nil {
		return nil, err
	}

	// TODO: this needs to be monotonically increasing. hybrid logical clocks?
	ts := time.Now().UnixNano() / 1000

	l := &pb.LogRecord{
		Mutation: &pb.Mutation{
			Key:       req.Key,
			Timestamp: ts,
			Value:     req.Value,
			Type:      pb.Mutation_PUT,
		},
	}

	ch := make(chan error, 1)

	trace.FromContext(ctx).Annotate(nil, "appending to log")
	d.logWriter.Append(l, func(err error) {
		trace.FromContext(ctx).Annotate(nil, "appending log done")
		if err != nil {
			ch <- err
			return
		}

		d.mu.Lock()
		d.apply(l)
		d.maybeTriggerFlush()
		d.mu.Unlock()

		ch <- nil
	})

	err := <-ch
	if err != nil {
		return nil, err
	}
	return &pb.SetResponse{Timestamp: ts}, nil
}

func (d *database) apply(l *pb.LogRecord) {
	m := l.Mutation
	switch m.Type {
	case pb.Mutation_PUT:
		d.memtable.Insert(l.Sequence, m.Key, m.Timestamp, m.Value)
	case pb.Mutation_DELETE:
		d.memtable.Insert(l.Sequence, m.Key, m.Timestamp, nil)
	default:
		glog.Fatalf("Mutation with unrecognized type: %v", m)
	}
}

func (d *database) maybeTriggerFlush() {
	if d.memtable.SizeBytes() > d.opts.MemtableFlushSize && d.imemtable == nil {
		d.swapMemtableLocked()
		go d.flushIMemtable()
	}
}

func (d *database) Find(ctx context.Context, key string) ([]byte, error) {
	// Acquire local copies of required structures, so that we can release lock quickly.
	d.mu.RLock()

	ssts := make([]*sst.Reader, len(d.ssts))
	for i, sst := range d.ssts {
		sst.Ref()
		ssts[i] = sst
	}
	defer func() {
		for _, sst := range ssts {
			sst.UnRef()
		}
	}()

	memtable := d.memtable
	imemtable := d.imemtable
	d.mu.RUnlock()

	v, found := memtable.Find(key)
	if found {
		return v, nil
	}
	if imemtable != nil {
		v, found = imemtable.Find(key)
		if found {
			return v, nil
		}
	}

	var value []byte
	valueTs := int64(math.MinInt64)

	for _, s := range ssts {
		v, ts, err := s.Find(ctx, key)
		if err == sst.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		if ts > valueTs {
			value = v
			valueTs = ts
		}
	}

	return value, nil
}

func (d *database) swapMemtableLocked() {
	m := d.memtable
	d.imemtable = m
	d.memtable = memtable.New(m.SequenceUpper())
}

func (d *database) flushIMemtable() {
	if d.imemtable == nil {
		glog.Fatalf("flushIMemtable called when imemtable == nil")
	}

	m := d.imemtable
	ts := time.Now().UnixNano()
	fn := fmt.Sprintf("%020d.sst", ts)
	fullFn := filepath.Join(d.opts.SstDir, fn)

	glog.Infof("flushing memtable of size %v to %v", m.SizeBytes(), fullFn)

	writer, err := sst.NewWriter(fullFn)
	if err != nil {
		glog.Fatalf("error opening SST while flushing memtable: %v", err)
	}
	it := m.NewIterator()
	for it.Next() {
		if err := writer.Append(it.Key(), it.Timestamp(), it.Value()); err != nil {
			glog.Fatalf("error appending SST while flushing memtable: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		glog.Fatalf("error closing SST while flushing memtable: %v", err)
	}

	glog.Infof("flush completed for %v", fullFn)
	// TODO: need to indicate that earlier log entries no longer needed.

	reader, err := sst.NewReader(fullFn, d.blockCache)
	if err != nil {
		glog.Fatalf("error opening SST that was just flushed: %v", err)
	}
	newSstMeta := &pb.SstMeta{Filename: fn, AppliedUntil: m.SequenceUpper()}

	d.mu.Lock()
	// Holding the db lock during descriptor save here - potentially slow.
	// Most DB operations (including mutations) probably only need a read lock on descriptor
	// so perhaps we need to finer-grained locking around the descriptor.
	d.descriptor.Current.SstMeta = append(d.descriptor.Current.SstMeta, newSstMeta)
	if err := d.descriptor.Save(); err != nil {
		glog.Fatalf("error saving descriptor while flushing memtable: %v", err)
	}
	d.imemtable = nil
	d.ssts = append(d.ssts, reader)
	d.mu.Unlock()
}

//func (d *database) cleanUnusedFiles() {
//}

// compactor monitors the number of SSTs, and triggers compaction when necessary.
// Currently the scheme is a very simple one - if there are more than 8 SSTs then compaction
// of all the SSTs is triggered.
func (d *database) compactor() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		var toCompact []*sst.Reader
		d.mu.RLock()
		if len(d.ssts) > 8 {
			toCompact = d.ssts
		}
		d.mu.RUnlock()

		if len(toCompact) > 0 {
			d.compact(toCompact)
		}
	}
}

// compact compacts ssts into a single SST and modifies the descriptor as appropriate.
func (d *database) compact(ssts []*sst.Reader) {
	ts := time.Now().UnixNano()
	fn := fmt.Sprintf("%020d.sst", ts)
	fullFn := filepath.Join(d.opts.SstDir, fn)

	glog.Infof("Compacting %v SSTs to %v", len(ssts), fullFn)
	if glog.V(4) {
		var names []string
		for _, sst := range ssts {
			names = append(names, sst.Filename())
		}
		glog.Infof("SSTs being compacted are %v", names)
	}

	iters := make([]Iter, len(ssts))
	for i, sst := range ssts {
		iter, err := sst.NewIter()
		if err != nil {
			glog.Fatalf("Error creating SST iter for compaction: %v", err)
		}
		iters[i] = iter
	}

	mIter, err := newMergingIter(iters)
	if err != nil {
		glog.Fatalf("Error creating merge iter: %v", err)
	}

	writer, err := sst.NewWriter(fullFn)
	if err != nil {
		glog.Fatalf("Error opening SST for writing: %v", err)
	}

	for {
		hasNext, err := mIter.Next()
		if err != nil {
			glog.Fatalf("Error writing to SST during compaction: %v", err)
		}
		if !hasNext {
			break
		}

		writer.Append(mIter.Key(), mIter.Timestamp(), mIter.Value())
	}

	mIter.Close()

	if err := writer.Close(); err != nil {
		glog.Fatalf("Error closing writer while compacting: %v", err)
	}

	glog.Infof("Compaction finished for %v", fullFn)

	filenames := make(map[string]bool)
	for _, sst := range ssts {
		filenames[sst.Filename()] = true
	}

	reader, err := sst.NewReader(fullFn, d.blockCache)
	if err != nil {
		glog.Fatalf("error opening freshly compacted SST %v: %v", fullFn, err)
	}

	d.mu.Lock()
	var newMetas []*pb.SstMeta
	maxApplied := int64(0)
	for _, meta := range d.descriptor.Current.SstMeta {
		if filenames[filepath.Join(d.opts.SstDir, meta.Filename)] {
			if meta.AppliedUntil > maxApplied {
				maxApplied = meta.AppliedUntil
			}
			continue
		}
		newMetas = append(newMetas, meta)
	}
	newMeta := &pb.SstMeta{Filename: fn, AppliedUntil: maxApplied}
	newMetas = append(newMetas, newMeta)
	d.descriptor.Current.SstMeta = newMetas
	if err := d.descriptor.Save(); err != nil {
		glog.Fatalf("error saving descriptor while flushing memtable: %v", err)
	}

	glog.V(4).Infof("Descriptor after compaction is: %v", d.descriptor)

	var newSsts []*sst.Reader
	for _, sst := range d.ssts {
		if filenames[sst.Filename()] {
			sst.UnRef()
			continue
		}
		newSsts = append(newSsts, sst)
	}
	newSsts = append(newSsts, reader)
	d.ssts = newSsts
	d.mu.Unlock()
}

func validateKey(k string) error {
	if k == "" {
		return status.Error(codes.InvalidArgument, "Key cannot be empty.")
	}
	if uint32(len(k)) > MaxKeySize {
		return status.Errorf(codes.InvalidArgument, "Key must be <= %d bytes", MaxKeySize)
	}
	return nil
}

func validateValue(v []byte) error {
	if uint32(len(v)) > MaxValueSize {
		return status.Errorf(codes.InvalidArgument, "Value must be <= %d bytes.", MaxValueSize)
	}
	return nil
}
