package leveldb

import (
	"bytes"

	"github.com/KierenEinar/leveldb/filter"
)

type iKeyFilter struct {
	uKeyFilter filter.IFilter
}

type iKeyFilterGenerator struct {
	uKeyGenerator filter.IFilterGenerator
}

func (iFilter iKeyFilter) MayContains(key, filter []byte) bool {
	return iFilter.uKeyFilter.MayContains(internalKey(key).userKey(), filter)
}

func (iFilter iKeyFilter) NewGenerator() filter.IFilterGenerator {
	return &iKeyFilterGenerator{
		uKeyGenerator: iFilter.uKeyFilter.NewGenerator(),
	}
}

func (iFilter iKeyFilter) Name() string {
	return "leveldb.builtin.bloomfilter"
}

func (iKeyFilterGenerator iKeyFilterGenerator) AddKey(key []byte) {
	iKeyFilterGenerator.uKeyGenerator.AddKey(internalKey(key).userKey())
}

func (iKeyFilterGenerator iKeyFilterGenerator) Generate(b *bytes.Buffer) {
	iKeyFilterGenerator.uKeyGenerator.Generate(b)
}

var (
	IFilter = &iKeyFilter{
		uKeyFilter: filter.DefaultFilter,
	}
)
