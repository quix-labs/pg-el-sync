package utils

import (
	"sync"
)

type ConcurrentSlice[T any] struct {
	rw    sync.RWMutex
	items []T
}

func (cs *ConcurrentSlice[T]) Append(item T) {
	cs.rw.Lock()
	defer cs.rw.Unlock()
	cs.items = append(cs.items, item)
}

func (cs *ConcurrentSlice[T]) All() []T {
	cs.rw.Lock()
	defer cs.rw.Unlock()
	return cs.items
}
func (cs *ConcurrentSlice[T]) Clear() {
	cs.rw.Lock()
	defer cs.rw.Unlock()
	cs.items = []T{}
}
func (cs *ConcurrentSlice[T]) Len() int {
	cs.rw.Lock()
	defer cs.rw.Unlock()
	return len(cs.items)
}
func (cs *ConcurrentSlice[T]) Retrieve(size int) []T {
	cs.rw.Lock()
	defer cs.rw.Unlock()

	maxSize := size
	if len(cs.items) < maxSize {
		maxSize = len(cs.items)
	}
	items := cs.items[:maxSize]
	cs.items = cs.items[maxSize:]
	return items
}
