package kafka

import (
	"context"
	"net"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int) error {
	if partitions <= 0 {
		partitions = 3
	}
	dialer := &kafkago.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := dialer.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
}
