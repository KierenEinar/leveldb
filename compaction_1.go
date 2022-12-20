package leveldb

import (
	"bytes"
	"leveldb/comparer"
	"leveldb/config"
	"leveldb/utils"
	"sort"
	"sync/atomic"
)

type compaction1 struct {
	inputs [2]tFiles
	levels Levels

	version *Version

	cPtr compactPtr

	// compaction grandparent level
	gp                tFiles
	gpOverlappedBytes int
	gpOverlappedLimit int

	inputOverlappedGPIndex int

	cmp comparer.Comparer

	minSeq         Sequence
	tWriter        *tWriter
	tableOperation *tableOperation
	edit           VersionEdit

	baseLevelI [config.KLevelNum]int
}

func (vSet *VersionSet) pickCompaction1() *compaction1 {
	sizeCompaction := vSet.current.cScore >= 1

	if !sizeCompaction {
		return nil
	}

	cLevel := vSet.current.cLevel
	utils.Assert(cLevel < config.KLevelNum)

	level := vSet.current.levels[cLevel]

	inputs := make(tFiles, 0)

	cPtr := vSet.compactPtrs[cLevel]
	if cPtr.ikey != nil {

		idx := sort.Search(len(level), func(i int) bool {
			return vSet.cmp.Compare(level[i].iMax, cPtr.ikey) > 0
		})

		if idx < len(level) {
			inputs = append(inputs, level[idx])
		}
	}

	if len(inputs) == 0 {
		inputs = append(inputs, level[0])
	}

	return newCompaction1(inputs, cPtr, vSet.current, vSet.tableOperation,
		vSet.current.levels, vSet.cmp)
}

func newCompaction1(inputs tFiles, cPtr compactPtr, version *Version, tableOperation *tableOperation,
	levels Levels, cmp BasicComparer) *compaction1 {
	version.Ref()
	c := &compaction1{
		inputs:            [2]tFiles{inputs},
		version:           version,
		levels:            levels,
		cPtr:              cPtr,
		cmp:               cmp,
		gpOverlappedLimit: defaultGPOverlappedLimit * defaultCompactionTableSize,
		tableOperation:    tableOperation,
	}
	c.expand()
	return c
}

func (c *compaction1) expand() {

	t0, t1 := c.inputs[0], c.inputs[1]

	vs0, vs1 := c.levels[c.cPtr.level], c.levels[c.cPtr.level+1]

	imin, imax := append(t0, t1...).getRange1(c.cmp)
	if c.cPtr.level == 0 {
		vs0.getOverlapped1(&t0, imin, imax, true)

		// recalculate the imin and imax
		imin, imax = append(t0, t1...).getRange1(c.cmp)
	}

	vs1.getOverlapped1(&t1, imin, imax, false)

	imin, imax = append(t0, t1...).getRange1(c.cmp)
	var tmpT0 tFiles
	vs0.getOverlapped1(&tmpT0, imin, imax, c.cPtr.level == 0)

	// see if we can expand the input 0 level file
	if len(tmpT0) > len(t0) {
		amin, amax := append(tmpT0, t1...).getRange1(c.cmp)
		var tmpT1 tFiles
		vs1.getOverlapped1(&tmpT1, amin, amax, false)
		// compact level must not change
		if len(tmpT1) == len(t1) && tmpT0.size()+vs1.size() < defaultCompactionTableSize*defaultCompactionExpandS0LimitFactor {
			t0 = tmpT0
			imin, imax = amin, amax
		}
	}

	// calculate the grand parent's
	gpLevel := c.cPtr.level + 2
	if gpLevel < kLevelNum {
		vs2 := c.levels[c.cPtr.level+2]
		vs2.getOverlapped1(&c.gp, imin, imax, false)
	}

}

func (tFiles tFiles) getOverlapped1(dst *tFiles, imin InternalKey, imax InternalKey, overlapped bool) {

	umin := imin.ukey()
	umax := imax.ukey()

	if overlapped {
		i := 0
		for ; i < len(tFiles); i++ {
			if tFiles[i].overlapped1(imin, imax) {
				tMinR := bytes.Compare(tFiles[i].iMin.ukey(), umin)
				tMaxR := bytes.Compare(tFiles[i].iMax.ukey(), umax)

				if tMinR >= 0 && tMaxR <= 0 {
					*dst = append(*dst, tFiles[i])
				} else {
					i = 0
					*dst = (*dst)[:0]
					if tMinR < 0 {
						umin = tFiles[i].iMin.ukey()
					}
					if tMaxR > 0 {
						umax = tFiles[i].iMax.ukey()
					}
				}
			}
		}
	} else {

		var (
			begin int
			end   int
		)

		idx := sort.Search(len(tFiles), func(i int) bool {
			return bytes.Compare(tFiles[i].iMin.ukey(), umin) <= 0
		})

		if idx == 0 {
			begin = 0
		} else if idx < len(tFiles) && bytes.Compare(tFiles[idx].iMax.ukey(), umin) <= 0 {
			begin -= 1
		} else {
			begin = idx
		}

		idx = sort.Search(len(tFiles), func(i int) bool {
			return bytes.Compare(tFiles[i].iMax.ukey(), umax) >= 0
		})

		if idx == len(tFiles) {
			end = idx
		} else if idx < len(tFiles) && bytes.Compare(tFiles[idx].iMin.ukey(), umax) <= 0 {
			end = idx + 1
		} else {
			end = idx
		}

		assert(end >= begin)
		*dst = append(*dst, tFiles[begin:end]...)
	}

}

func (tFile tFile) overlapped1(imin InternalKey, imax InternalKey) bool {
	if bytes.Compare(tFile.iMax.ukey(), imin.ukey()) < 0 ||
		bytes.Compare(tFile.iMin.ukey(), imax.ukey()) > 0 {
		return false
	}
	return true
}

func (tFiles tFiles) getRange1(cmp BasicComparer) (imin, imax InternalKey) {
	for _, tFile := range tFiles {
		if cmp.Compare(tFile.iMin, imin) < 0 {
			imin = tFile.iMin
		}
		if cmp.Compare(tFile.iMax, imax) > 0 {
			imax = tFile.iMax
		}
	}
	return
}

func (db *DB) doCompactionWork(c *compaction1) error {

	if db.VersionSet.snapshots.Len() == 0 {
		c.minSeq = db.seqNum
	} else {
		c.minSeq = db.VersionSet.snapshots.Front().Value.(Sequence)
	}

	iter, iterErr := db.VersionSet.makeInputIterator(c)
	if iterErr != nil {
		return iterErr
	}

	db.rwMutex.Unlock()

	var (
		drop     bool
		err      error
		lastIKey InternalKey
		lastSeq  Sequence
	)

	for iter.Next() && iter.Valid() == nil && atomic.LoadUint32(&db.shutdown) == 0 {

		if atomic.LoadUint32(&db.hasImm) == 1 {
			db.rwMutex.Lock()
			db.compactMemTable()
			db.backgroundWorkFinishedSignal.Broadcast()
			db.rwMutex.Unlock()
		}

		inputKey := iter.Key()
		value := iter.Value()
		// if current file input key will expand the overlapped with grand parent,
		// it need to finish current table and create a new one
		if c.tWriter != nil && c.shouldStopBefore(inputKey) {
			if err = db.finishCompactionOutputFile(c); err != nil {
				break
			}
		}

		uk, kt, seq, parseErr := parseInternalKey(inputKey)

		if parseErr != nil {
			lastIKey = append([]byte(nil), inputKey...)
			drop = false
		} else {
			// ukey first occur
			if lastIKey == nil || bytes.Compare(lastIKey.ukey(), uk) != 0 {
				lastSeq = Sequence(kMaxSequenceNum)
				lastIKey = append([]byte(nil), inputKey...)
			}
			if lastSeq > c.minSeq {
				drop = false
				if kt == keyTypeValue {

				} else if kt == keyTypeDel && Sequence(seq) < c.minSeq && c.isBaseLevelForKey(inputKey) {
					drop = true
				}
			} else {
				drop = true
			}
			lastSeq = Sequence(seq)
		}

		if drop {
			continue
		}

		if c.tWriter != nil && c.tWriter.size() > defaultCompactionTableSize {
			if err = db.finishCompactionOutputFile(c); err != nil {
				break
			}
		}

		if c.tWriter == nil {
			if c.tWriter, err = c.tableOperation.create(); err != nil {
				break
			}
		}

		if err = c.tWriter.append(inputKey, value); err != nil {
			break
		}
	}

	if c.tWriter != nil {
		err = db.finishCompactionOutputFile(c)
	}

	iter.UnRef()

	db.rwMutex.Lock()

	if err == nil {
		err = db.VersionSet.logAndApply(&c.edit, &db.rwMutex)
	}

	return err

}

func (vSet *VersionSet) makeInputIterator(c *compaction1) (iter Iterator, err error) {

	iters := make([]Iterator, 0)

	defer func() {
		if err != nil {
			for _, iter := range iters {
				iter.UnRef()
			}
			iters = nil
		}
	}()

	for which, inputs := range c.inputs {
		if c.cPtr.level+which == 0 {
			for _, input := range inputs {
				iter, err := vSet.newTableIterator(input)
				if err != nil {
					return
				}
				iters = append(iters, iter)
			}
			iters = append(iters, iter)
		} else {
			iters = append(iters, newIndexedIterator(newTFileArrIteratorIndexer(inputs)))
		}
	}

	iter = NewMergeIterator(iters)

	return
}

func (vSet *VersionSet) newTableIterator(tFile tFile) (Iterator, error) {

	reader, err := vSet.storage.Open(tFile.fd)
	if err != nil {
		return nil, err
	}
	tr, err := NewTableReader(reader, tFile.Size)
	if err != nil {
		return nil, err
	}

	iter, err := tr.NewIterator()
	if err != nil {
		return nil, err
	}

	return iter, nil
}

func (c *compaction1) shouldStopBefore(nextKey InternalKey) bool {

	for c.inputOverlappedGPIndex < len(c.gp) {
		if c.cmp.Compare(nextKey, c.gp[c.inputOverlappedGPIndex].iMax) > 0 {
			c.gpOverlappedBytes += c.gp[c.inputOverlappedGPIndex].Size
			c.inputOverlappedGPIndex++
			if c.gpOverlappedBytes > c.gpOverlappedLimit {
				c.gpOverlappedBytes = 0
				return true
			}
		} else {
			break
		}
	}
	return false
}

func (db *DB) finishCompactionOutputFile(c *compaction1) error {
	assert(c.tWriter != nil)

	tFile, err := c.tWriter.finish()
	if err != nil {
		return err
	}
	c.edit.addNewTable(c.cPtr.level+1, tFile.Size, tFile.fd.Num, tFile.iMin, tFile.iMax)
	c.tWriter = nil
	return nil
}

func (c *compaction1) isBaseLevelForKey(input InternalKey) bool {
	for levelI := c.cPtr.level + 2; levelI < len(c.levels); levelI++ {
		level := c.levels[levelI]

		for c.baseLevelI[levelI] < len(level) {
			table := level[c.baseLevelI[levelI]]
			if bytes.Compare(input.ukey(), table.iMax.ukey()) > 0 {
				c.baseLevelI[levelI]++
			} else if bytes.Compare(input.ukey(), table.iMin.ukey()) < 0 {
				break
			} else {
				return false
			}
		}
	}
	return true
}

func (c *compaction1) releaseInputs() {
	c.version.UnRef()
}
