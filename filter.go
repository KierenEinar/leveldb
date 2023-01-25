package leveldb

import (
	"bytes"
	"leveldb/filter"
)

type iKeyFilter struct {
	uKeyFilter filter.IFilter
}

type iKeyFilterGenerator struct {
	uKeyGenerator filter.IFilterGenerator
}

func (iFilter iKeyFilter) MayContains(key, filter []byte) bool {
	return iFilter.uKeyFilter.MayContains(InternalKey(key).userKey(), filter)
}

func (iFilter iKeyFilter) NewGenerator() filter.IFilterGenerator {
	return &iKeyFilterGenerator{}
}

func (iFilter iKeyFilter) Name() string {
	return "leveldb.builtin.bloomfilter"
}

func (iKeyFilterGenerator iKeyFilterGenerator) AddKey(key []byte) {
	iKeyFilterGenerator.uKeyGenerator.AddKey(InternalKey(key).userKey())
}

func (iKeyFilterGenerator iKeyFilterGenerator) Generate(b *bytes.Buffer) {
	iKeyFilterGenerator.uKeyGenerator.Generate(b)
}

var (
	IFilter = &iKeyFilter{}
)
