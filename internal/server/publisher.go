package server

import (
	"broker/internal/broker"
	brokerv1 "broker/proto/v1"
	"io"
	"sync/atomic"
)

type PublisherServer struct{ 
	brokerv1.UnimplementedEventBrokerServiceServer
	router *broker.Router
	eventSeq uint64
}

func NewPublisherServer(router *broker.Router) *PublisherServer { 
	return &PublisherServer{router: router,}
}

func (s *PublisherServer) PublishStream(stream brokerv1.EventBrokerService_PublishStreamServer) error { 
	for {
		req, err := stream.Recv()
		if err == io.EOF { 
			return nil
		} 
		if err != nil { 
			return err 
		}

		eventId := atomic.AddUint64(&s.eventSeq, 1)

		s.router.RouteIngress(
			req.GetTopic(),
			req.GetKey(),
			req.GetPayload(),
			req.GetHeaders(),
		)

		err = stream.Send(&brokerv1.PublishAck{
			EventId: eventId,
			Success: true,
		})
		if err != nil { 
			return err 
		}
	}
}