package consumer

import (
	"github.com/Shopify/sarama"
)

type MessageHandler interface {
	Handle(*producer.BinlogEvent)
}

func initConsumer() *sarama.PartitionConsumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Specify brokers address. This is default one
	brokers := []string{"localhost:9092"}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	topic := "important"
	// How to decide partition, is it fixed value...?
	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	return consumer
}

func main() {

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Println("Received messages", string(msg.Key), string(msg.Value))
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}

func (con *DefaultConsumer) Consume(msg *sarama.ConsumerMessage) {
	event := &producer.BinlogEvent{}
	if err := json.Unmarshal(msg.Value, event); err != nil {
		panic(err.Error())
	}
	switch event.Header.EventType {
	// case INSERT_ROWS
	}
	ev := event.Event.(map[string]interface{})
	tmap := ev["Table"].(map[string]interface{})
	rows := ev["Rows"].([]interface{})
	if tmap["Table"] == "users" {
		for _, row := range rows {
			rowary, _ := row.([]interface{})
			_, err := db.Exec(
				"insert into users (user_id, email, user_name) values (?, ?, ?)",
				rowary[0],
				rowary[1],
				rowary[2],
			)
			if err != nil {
				log.Println(err.Error())
			}
		}
	}
}

type RowsEvent struct {
	Table struct {
		Name        string
		ColumnOrder []string
		ColumnDef   map[string]string
	}
}

type QueryValues struct {
	MapFrom struct {
		Table string
	}
	MapTo struct {
		Table       string
		ColumnOrder []string
		ColumnDef   map[string]string
	}
	Rows  []interface{}
	Table string
}
