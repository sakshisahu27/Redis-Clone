package main

import (
	"errors"
	"log"
	"sort"
	"sync"
	"time"
)

type Database struct {
	store map[string]*Item
	mu    sync.RWMutex
	mem   int64
}

func NewDatabase() *Database {
	return &Database{
		store: map[string]*Item{},
		mu:    sync.RWMutex{},
	}
}

func (db *Database) evictKeys(state *AppState, requiredMem int64) error {
	if state.conf.eviction == NoEviction {
		return errors.New("maximum memory reached")
	}

	samples := sampleKeys(state)

	enoughMemFreed := func() bool {
		if db.mem + requiredMem <= state.conf.maxmem {
			return true
		}
		return false
	}

	evictUntilMemFreed := func(samples []sample) {
		for _, s := range samples {
			log.Println("evicting: ", s.k)
			db.Delete(s.k)
			if enoughMemFreed() {
				break
			}
		}
	}

	switch state.conf.eviction {
	case AllKeysRandom:
		evictUntilMemFreed(samples)
	case AllKeysLRU:
		sort.Slice(samples, func(i, j int) bool {
			return samples[i].v.LastAccess.After(samples[j].v.LastAccess)
		})
		evictUntilMemFreed(samples)
	case AllKeysLFU:
		sort.Slice(samples, func(i, j int) bool {
			return samples[i].v.Accessess < samples[j].v.Accessess
		})
		evictUntilMemFreed(samples)
	}

	return nil
}

func (db *Database) tryExpire(k string, i *Item) bool {
	if i.shouldExpire() {
		DB.mu.Lock()
		DB.Delete(k)
		DB.mu.Unlock()
		return true
	}
	return false
}

func (db *Database) Get(k string) (i *Item, ok bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	item, ok := db.store[k]
	if !ok {
		return item, ok
	}
	expired := db.tryExpire(k, item)
	if expired {
		return &Item{}, false
	}

	item.Accessess++
	item.LastAccess = time.Now()

	log.Printf("item %s accessed %d times at: %v", k, item.Accessess, item.LastAccess)
	return item, ok
}

func (db *Database) Set(k string, v string, state *AppState) error {
	if old, ok := db.store[k]; ok {
		oldmem := old.approxMemUsage(k)
		db.mem -= oldmem
	}
	key := &Item{V: v}
	kmem := key.approxMemUsage(k)

	outOfMem := state.conf.maxmem > 0 && db.mem+kmem > state.conf.maxmem
	if outOfMem {
		err := db.evictKeys(state, kmem)
		if err != nil {
			return err
		}
	}

	db.store[k] = key
	db.mem += kmem
	log.Println("memory: ", db.mem)

	return nil
}

func (db *Database) Delete(k string) {
	key, ok := db.store[k]
	if !ok {
		return //fail gracefully
	}
	kmem := key.approxMemUsage(k)

	delete(db.store, k)
	db.mem -= kmem
	log.Println("memory: ", db.mem)
}

var DB = NewDatabase()
