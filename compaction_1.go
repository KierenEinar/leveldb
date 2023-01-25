package leveldb

import (
	"bytes"
	"leveldb/comparer"
	"leveldb/errors"
	"leveldb/iterator"
	"leveldb/options"
	"leveldb/utils"
	"sort"
	"sync/atomic"
)

type compaction1 struct {
	inputs      [2]tFiles
	levels      Levels
	sourceLevel int
	version     *Version

	cPtr compactPtr

	// compaction grandparent level
	gp                tFiles
	gpOverlappedBytes int
	gpOverlappedLimit int

	inputOverlappedGPIndex int

	opt            *options.Options
	outputs        map[uint64]struct{}
	minSeq         sequence
	tWriter        *tWriter
	tableOperation *tableOperation
	tableCache     *TableCache
	edit           VersionEdit

	baseLevelI [options.KLevelNum]int

	err error
}

func (vSet *VersionSet) pickCompaction1() *compaction1 {

	version := vSet.current
	version.Ref()

	sizeCompaction := version.cScore >= 1
	seekCompaction := version.fileToCompact != nil
	inputs := make(tFiles, 0)
	var sourceLevel int
	if sizeCompaction {
		level := vSet.current.levels[version.cLevel]
		cPtr := vSet.compactPtrs[version.cLevel]
		sourceLevel = version.cLevel
		if version.cLevel > 0 && cPtr != nil {
			idx := sort.Search(len(level), func(i int) bool {
				return vSet.opt.InternalComparer.Compare(level[i].iMax, cPtr.ikey) > 0
			})
			if idx < len(level) {
				inputs = append(inputs, level[idx])
			}
		}
		if len(inputs) == 0 {
			inputs = append(inputs, level[0])
		}
	} else if seekCompaction {
		inputs = append(inputs, *version.fileToCompact)
		sourceLevel = version.fileToCompactLevel
	}

	return newCompaction1(inputs, sourceLevel, version, vSet.tableOperation,
		vSet.current.levels, vSet.opt)
}

func newCompaction1(inputs tFiles, sourceLevel int, version *Version, tableOperation *tableOperation,
	levels Levels, opt *options.Options) *compaction1 {
	c := &compaction1{
		inputs:            [2]tFiles{inputs},
		sourceLevel:       sourceLevel,
		version:           version,
		levels:            levels,
		opt:               opt,
		gpOverlappedLimit: opt.GPOverlappedLimit * int(opt.MaxEstimateFileSize),
		tableOperation:    tableOperation,
	}
	c.expand()
	return c
}

func (c *compaction1) expand() {

	t0, t1 := c.inputs[0], c.inputs[1]

	vs0, vs1 := c.levels[c.sourceLevel], c.levels[c.sourceLevel+1]

	imin, imax := append(t0, t1...).getRange1(c.opt.InternalComparer)
	if c.sourceLevel == 0 {
		vs0.getOverlapped1(&t0, imin, imax, true)

		// recalculate the imin and imax
		imin, imax = append(t0, t1...).getRange1(c.opt.InternalComparer)
	}

	vs1.getOverlapped1(&t1, imin, imax, false)

	imin, imax = append(t0, t1...).getRange1(c.opt.InternalComparer)
	var tmpT0 tFiles
	vs0.getOverlapped1(&tmpT0, imin, imax, c.sourceLevel == 0)

	// see if we can expand the input 0 level file
	if len(tmpT0) > len(t0) {
		amin, amax := append(tmpT0, t1...).getRange1(c.opt.InternalComparer)
		var tmpT1 tFiles
		vs1.getOverlapped1(&tmpT1, amin, amax, false)
		// compact level must not change
		if len(tmpT1) == len(t1) && tmpT0.size()+vs1.size() < int(c.opt.MaxEstimateFileSize*c.opt.MaxCompactionLimitFactor) {
			t0 = tmpT0
			imin, imax = amin, amax
		}
	}

	// calculate the grand parent's
	gpLevel := c.sourceLevel + 2
	if gpLevel < options.KLevelNum {
		vs2 := c.levels[gpLevel]
		vs2.getOverlapped1(&c.gp, imin, imax, false)
	}

}

func (tFiles tFiles) getOverlapped1(dst *tFiles, imin internalKey, imax internalKey, overlapped bool) {

	umin := imin.userKey()
	umax := imax.userKey()

	if overlapped {
		i := 0
		for ; i < len(tFiles); i++ {
			if tFiles[i].overlapped1(imin, imax) {
				tMinR := bytes.Compare(tFiles[i].iMin.userKey(), umin)
				tMaxR := bytes.Compare(tFiles[i].iMax.userKey(), umax)

				if tMinR >= 0 && tMaxR <= 0 {
					*dst = append(*dst, tFiles[i])
				} else {
					i = 0
					*dst = (*dst)[:0]
					if tMinR < 0 {
						umin = tFiles[i].iMin.userKey()
					}
					if tMaxR > 0 {
						umax = tFiles[i].iMax.userKey()
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
			return bytes.Compare(tFiles[i].iMin.userKey(), umin) <= 0
		})

		if idx == 0 {
			begin = 0
		} else if idx < len(tFiles) && bytes.Compare(tFiles[idx].iMax.userKey(), umin) <= 0 {
			begin -= 1
		} else {
			begin = idx
		}

		idx = sort.Search(len(tFiles), func(i int) bool {
			return bytes.Compare(tFiles[i].iMax.userKey(), umax) >= 0
		})

		if idx == len(tFiles) {
			end = idx
		} else if idx < len(tFiles) && bytes.Compare(tFiles[idx].iMin.userKey(), umax) <= 0 {
			end = idx + 1
		} else {
			end = idx
		}

		utils.Assert(end >= begin)
		*dst = append(*dst, tFiles[begin:end]...)
	}

}

func (tFile tFile) overlapped1(imin internalKey, imax internalKey) bool {
	if bytes.Compare(tFile.iMax.userKey(), imin.userKey()) < 0 ||
		bytes.Compare(tFile.iMin.userKey(), imax.userKey()) > 0 {
		return false
	}
	return true
}

func (tFiles tFiles) getRange1(cmp comparer.Comparer) (imin, imax internalKey) {
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

func (dbImpl *DBImpl) doCompactionWork(c *compaction1) (err error) {

	utils.AssertMutexHeld(&dbImpl.rwMutex)

	defer func() {
		if err != nil {
			c.err = err
		}
	}()

	if dbImpl.snapshots.Len() == 0 {
		c.minSeq = dbImpl.seqNum
	} else {
		c.minSeq = dbImpl.snapshots.Front().Value.(sequence)
	}

	dbImpl.rwMutex.Unlock()

	iter, iterErr := c.makeInputIterator()
	if iterErr != nil {
		err = iterErr
		return
	}
	defer iter.UnRef()

	var (
		drop     bool
		lastIKey internalKey
		lastSeq  sequence
	)

	for iter.Next() && iter.Valid() == nil && atomic.LoadUint32(&dbImpl.shutdown) == 0 {

		if atomic.LoadUint32(&dbImpl.hasImm) == 1 {
			dbImpl.rwMutex.Lock()
			dbImpl.compactMemTable()
			dbImpl.backgroundWorkFinishedSignal.Broadcast()
			dbImpl.rwMutex.Unlock()
		}

		inputKey := iter.Key()
		value := iter.Value()
		// if current file input key will expand the overlapped with grand parent,
		// it need to finish current table and create a new one
		if c.tWriter != nil && c.shouldStopBefore(inputKey) {
			if err = dbImpl.finishCompactionOutputFile(c); err != nil {
				break
			}
		}

		uk, kt, seq, parseErr := parseInternalKey(inputKey)

		if parseErr != nil {
			lastIKey = inputKey
			drop = false
		} else {
			// ukey first occur
			if lastIKey == nil || bytes.Compare(lastIKey.userKey(), uk) != 0 {
				lastSeq = sequence(kMaxSequenceNum)
				lastIKey = inputKey
			}
			if lastSeq > c.minSeq {
				drop = false
				if kt == keyTypeValue {

				} else if kt == keyTypeDel && sequence(seq) < c.minSeq && c.isBaseLevelForKey(inputKey) {
					drop = true
				}
			} else {
				drop = true
			}
			lastSeq = sequence(seq)
		}

		if drop {
			continue
		}

		if c.tWriter != nil && c.tWriter.size() > int(c.opt.MaxEstimateFileSize) {
			if err = dbImpl.finishCompactionOutputFile(c); err != nil {
				break
			}
		}

		if c.tWriter == nil {
			dbImpl.rwMutex.Lock()
			nextFd := dbImpl.versionSet.allocFileNum()
			dbImpl.pendingOutputs[nextFd] = struct{}{}
			dbImpl.rwMutex.Unlock()
			fileMeta := tFile{
				fd: int(nextFd),
			}
			c.outputs[nextFd] = struct{}{}
			if c.tWriter, err = c.tableOperation.create(fileMeta); err != nil {
				break
			}
		}

		if err = c.tWriter.append(inputKey, value); err != nil {
			break
		}
	}

	if err == nil && c.tWriter != nil {
		err = dbImpl.finishCompactionOutputFile(c)
	}

	dbImpl.rwMutex.Lock()

	if err == nil {

		if atomic.LoadUint32(&dbImpl.shutdown) == 1 {
			return errors.ErrClosed
		}

		for _, input := range c.inputs {
			for which, table := range input {
				c.edit.addDelTable(which+c.sourceLevel, uint64(table.fd))
			}
		}

		err = dbImpl.versionSet.logAndApply(&c.edit, &dbImpl.rwMutex)
	}
	return

}

func (c *compaction1) makeInputIterator() (iter iterator.Iterator, err error) {

	iters := make([]iterator.Iterator, 0)

	defer func() {
		if err != nil {
			for _, iter := range iters {
				iter.UnRef()
			}
		}
	}()

	for which, inputs := range c.inputs {
		if c.sourceLevel+which == 0 {
			for _, input := range inputs {
				iter, err := c.tableCache.NewIterator(input)
				if err != nil {
					return
				}
				iters = append(iters, iter)
			}
		} else {
			iters = append(iters, iterator.NewIndexedIterator(newTFileArrIteratorIndexer(inputs, c.opt.InternalComparer)))
		}
	}

	iter = iterator.NewMergeIterator(iters, c.opt.InternalComparer)

	return
}

func (c *compaction1) shouldStopBefore(nextKey internalKey) bool {

	for c.inputOverlappedGPIndex < len(c.gp) {
		if c.opt.InternalComparer.Compare(nextKey, c.gp[c.inputOverlappedGPIndex].iMax) > 0 {
			c.gpOverlappedBytes += c.gp[c.inputOverlappedGPIndex].size
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

func (dbImpl *DBImpl) finishCompactionOutputFile(c *compaction1) error {
	utils.Assert(c.tWriter != nil)

	err := c.tWriter.finish()
	if err != nil {
		return err
	}
	fileMeta := c.tWriter.fileMeta
	c.edit.addNewTable(c.sourceLevel+1, fileMeta.size, uint64(fileMeta.fd), fileMeta.iMin, fileMeta.iMax)
	c.tWriter = nil
	return nil
}

func (c *compaction1) isBaseLevelForKey(input internalKey) bool {
	for levelI := c.sourceLevel + 2; levelI < len(c.levels); levelI++ {
		level := c.levels[levelI]

		for c.baseLevelI[levelI] < len(level) {
			table := level[c.baseLevelI[levelI]]
			if bytes.Compare(input.userKey(), table.iMax.userKey()) > 0 {
				c.baseLevelI[levelI]++
			} else if bytes.Compare(input.userKey(), table.iMin.userKey()) < 0 {
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
