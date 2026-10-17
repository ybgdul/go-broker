package server

import (
	"broker/internal/broker"
	"broker/models"
	brokerv1 "broker/proto/v1"
)

type SubscribeServer struct{ 
	brokerv1.UnimplementedEventBrokerServiceServer
	router *broker.Router
}

func NewSubscribeServer(router *broker.Router) *SubscribeServer{ 
	return &SubscribeServer{
		router: router,
	}
}

func (s *SubscribeServer) SubscribeStream(req *brokerv1.SubscribeRequest, stream brokerv1.EventBrokerService_SubscribeStreamServer) error { 
	ctx := stream.Context()

	var strategy models.Strategy
	switch req.GetBackpressure(){
	case brokerv1.Strategy_DROP_OLDEST:
		strategy = models.StrategyDropOldest
	default:
		strategy = models.StrategyDropNewest
	}

	capacity := req.GetQueueCapacity()
	if capacity == 0 {
		capacity = 4096
	}

	sub := &models.Subscriber{
		ID: req.GetClientId(),
		Topics: req.GetTopicPatterns(),
		Queue: make(chan *models.Event, capacity),
		Strategy: strategy,
	}

	s.router.RegisterSubscriber(sub)
	defer func() {
		s.router.UnregisterSubscriber(sub.ID)
		close(sub.Queue)
		for event := range sub.Queue {
			event.Free()
		}
	}()

	for { 
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event, ok := <-sub.Queue:
			if !ok { 
				return nil
			}

			err := stream.Send(&brokerv1.EventEnvelope{
				EventId: event.ID,
				Topic: event.Topic,
				Key: event.Key,
				Payload: event.Payload,
				TimestampNs: event.Timestamp.UnixNano(),
				Headers: event.Headers,
			})

			event.Free()

			if err != nil { 
				return err
			}
		}
	}

}