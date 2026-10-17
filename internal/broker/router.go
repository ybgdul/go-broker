package broker

import (
	"broker/models"
	"hash/fnv"
	"sync"
)

type Router struct {
	partitionsCount uint32 
	partitions []*models.Partition
	eventPool *sync.Pool
}

func NewRouter(count uint32) *Router { 
	router := &Router{
		partitionsCount: count,
		partitions: make([]*models.Partition, count),
		eventPool: &sync.Pool{
			New: func() any { 
				return &models.Event{
					Headers: make(map[string]string),
				}
			},
		},
	}

	for i := uint32(0); i < count; i++ { 
		router.partitions[i] = models.NewPartition(i)
	}
	return router
}

func (r *Router) RouteIngress(topic string, key string, payload []byte, headers map[string]string ) uint64 { 
	event := models.NewEventFromPool(r.eventPool)
	event.Topic = topic
	event.Key = key 
	event.Payload = append(event.Payload[:0], payload...)
	for k, v := range headers { 
		event.Headers[k] = v
	}

	partitionID := r.hash(key) % r.partitionsCount
	partition := r.partitions[partitionID]

	partition.PublishFansOut(event)
	event.Free()

	return event.ID
}

func (r *Router) hash(key string) uint32 { 
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (r *Router) RegisterSubscriber(sub *models.Subscriber) {
	for _, p := range r.partitions{
		p.RegisterSubscriber(sub)
	}

}

func (r *Router) UnregisterSubscriber(id string) {
	for _, p := range r.partitions{
		p.DeleteSubscriber(id)
	}
}