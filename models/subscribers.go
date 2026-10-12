package models

import "sync/atomic"

type Strategy uint8 

const ( 
	StrategyDropNewest Strategy = iota 
	StrategyDropOldest 
	StrategyBlockWithTimeout
)

type Subscriber struct{ 
	ID string 
	Topics []string
	Queue chan *Event
	Strategy Strategy
	Delivered uint64 
	Dropped uint64
}

func (s *Subscriber) Deliver(e *Event) {
	e.Retain()

	select { 
	case s.Queue<- e: 
		atomic.AddUint64(&s.Delivered, 1)
	default:
		s.HandleBackpressure(e)
	}
}

func (s *Subscriber) HandleBackpressure(e *Event) { 
	switch s.Strategy { 
	case StrategyDropNewest:
		atomic.AddUint64(&s.Dropped, 1)
		e.Free()
	case StrategyDropOldest: 
		select{ 
		case old := <-s.Queue:
			old.Free()
		default:
		}

		select { 
		case s.Queue<-e: 
			atomic.AddUint64(&s.Delivered, 1)
		default: 
			atomic.AddUint64(&s.Dropped, 1)
			e.Free()
		}
	}
}
