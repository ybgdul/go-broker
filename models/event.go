package models

import (
	"sync"
	"sync/atomic"
	"time"
)

type Event struct{
	ID uint64
	Topic string 
	Key string 
	Payload []byte
	Timestamp time.Time

	refCount int32
	pool *sync.Pool
}

func (e *Event) Retain() { 
	atomic.AddInt32(&e.refCount, 1)
}

func (e *Event) Free() { 
	if atomic.AddInt32(&e.refCount, -1) == 0{ 
		e.Payload = e.Payload[:0]
		e.Topic = ""
		e.Key = ""
		e.pool.Put(e)
	}
}

func NewEventFromPool(pool *sync.Pool) *Event { 
	event := pool.Get().(*Event)
	atomic.StoreInt32(&event.refCount, 1)
	event.pool = pool
	return event
} 


