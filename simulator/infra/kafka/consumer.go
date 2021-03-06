package kafka

import (
	"fmt"
	"log"
	"os"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConsumer struct {
	MsgChan chan *ckafka.Message
}

func NewKafkaConsumer(msgChan chan *ckafka.Message) *KafkaConsumer {
	return &KafkaConsumer{
		MsgChan: msgChan,
	}
}

func (k *KafkaConsumer) Consume() {
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		"group.id":          os.Getenv("KAFKA_CONSUMER_GROUP_ID"),
		"security.protocol": os.Getenv("security.protocol"),
		"sasl.mechanisms":   os.Getenv("sasl.mechanisms"),
		"sasl.username":     os.Getenv("sasl.username"),
		"sasl.password":     os.Getenv("sasl.password"),
	}
	c, err := ckafka.NewConsumer(configMap)
	if err != nil {
		log.Fatalf("Error consuming kafka message: " + err.Error())
	}
	topics := []string{os.Getenv("KAFKA_READ_TOPIC")}
	c.SubscribeTopics(topics, nil)
	fmt.Println("Kafka Consumer has been started.")

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			k.MsgChan <- msg
		}
	}
}
