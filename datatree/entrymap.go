package datatree

import (
	"sync"
)

type EntryMap struct {
	m sync.Map
}

func (em *EntryMap) Delete(key int64) {
	em.m.Delete(key)
}

func (em *EntryMap) Load(key int64) (value *Entry, ok bool) {
	v, ok := em.m.Load(key)
	value = v.(*Entry)
}

func (em *EntryMap) Store(key int64, value *Entry) {
	em.m.Store(key, value)
}

func (em *EntryMap) Clear() {
	em.m.Range(func(key, value interface{}) bool {
		em.m.Delete(key)
		return true
	})
}

