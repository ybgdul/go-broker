package models

import "sync"

type Partition struct{ 
	ID uint32
	mu sync.RWMutex
	subscribers map[string]*Subscriber
}

func NewPartition(id uint32) *Partition { 
	return &Partition{
		ID: id,
		subscribers: make(map[string]*Subscriber),
	}
}

func (p *Partition) RegisterSubscriber(s *Subscriber) { 
	p.mu.Lock()
	p.subscribers[s.ID] = s
	p.mu.Unlock()
}

func (p *Partition) DeleteSubscriber(id string) { 
	p.mu.Lock()
	delete(p.subscribers, id)
	p.mu.Unlock()
}

func (p *Partition) PublishFansOut(e *Event) { 
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, sub := range p.subscribers { 
		if matchTopics(e.Topic, sub.Topics) { 
			sub.Deliver(e)
		}
	}
}

func matchTopics(topic string, patterns []string) bool { 
	for _, pattern := range patterns { 
		if pattern == "*" || pattern == topic { 
			return true
		}
	}
	return false
}