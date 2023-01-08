package leveldb

import (
	"bytes"
	"leveldb/comparer"
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

	opt *options.Options

	minSeq         Sequence
	tWriter        *tWriter
	tableOperation *tableOperation
	tableCache     *TableCache
	edit           VersionEdit

	baseLevelI [options.KLevelNum]int
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

func (tFiles tFiles) getOverlapped1(dst *tFiles, imin InternalKey, imax InternalKey, overlapped bool) {

	umin := imin.UserKey()
	umax := imax.UserKey()

	if overlapped {
		i := 0
		for ; i < len(tFiles); i++ {
			if tFiles[i].overlapped1(imin, imax) {
				tMinR := bytes.Compare(tFiles[i].iMin.UserKey(), umin)
				tMaxR := bytes.Compare(tFiles[i].iMax.UserKey(), umax)

				if tMinR >= 0 && tMaxR <= 0 {
					*dst = append(*dst, tFiles[i])
				} else {
					i = 0
					*dst = (*dst)[:0]
					if tMinR < 0 {
						umin = tFiles[i].iMin.UserKey()
					}
					if tMaxR > 0 {
						umax = tFiles[i].iMax.UserKey()
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
			return bytes.Compare(tFiles[i].iMin.UserKey(), umin) <= 0
		})

		if idx == 0 {
			begin = 0
		} else if idx < len(tFiles) && bytes.Compare(tFiles[idx].iMax.UserKey(), umin) <= 0 {
			begin -= 1
		} else {
			begin = idx
		}

		idx = sort.Search(len(tFiles), func(i int) bool {
			return bytes.Compare(tFiles[i].iMax.UserKey(), umax) >= 0
		})

		if idx == len(tFiles) {
			end = idx
		} else if idx < len(tFiles) && bytes.Compare(tFiles[idx].iMin.UserKey(), umax) <= 0 {
			end = idx + 1
		} else {
			end = idx
		}

		utils.Assert(end >= begin)
		*dst = append(*dst, tFiles[begin:end]...)
	}

}

func (tFile tFile) overlapped1(imin InternalKey, imax InternalKey) bool {
	if bytes.Compare(tFile.iMax.UserKey(), imin.UserKey()) < 0 ||
		bytes.Compare(tFile.iMin.UserKey(), imax.UserKey()) > 0 {
		return false
	}
	return true
}

func (tFiles tFiles) getRange1(cmp comparer.Comparer) (imin, imax InternalKey) {
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

func (dbImpl *DBImpl) doCompactionWork(c *compaction1) error {

	if dbImpl.snapshots.Len() == 0 {
		c.minSeq = dbImpl.seqNum
	} else {
		c.minSeq = dbImpl.snapshots.Front().Value.(Sequence)
	}

	iter, iterErr := c.makeInputIterator()
	if iterErr != nil {
		return iterErr
	}
	defer iter.UnRef()

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

	db.rwMutex.Lock()

	if err == nil {
		err = db.VersionSet.logAndApply(&c.edit, &db.rwMutex)
	}

	return err

}

func (c *compaction1) makeInputIterator() (iter iterator.Iterator, err error) {

	iters := make([]iterator.Iterator, 0)

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

func (vSet *VersionSet) newTableIterator(tFile tFile) (Iterator, error) {

	reader, err := vSet.opt.Storage.Open(tFile.fd)
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
