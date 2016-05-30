package consumer

import (
	"github.com/Shopify/sarama"
)

type BinlogConsumer interface {
}

type BinlogConsumerMessage struct {
}

type MessageHandler interface {
	InsertRows(msg *BinlogConsumerMessage) error
	UpdateRows(msg *BinlogConsumerMessage) error
	DeleteRows(msg *BinlogConsumerMessage) error
}

// TODO: implement
// logfile, logpostion からparition, offsetを取得する
func DetectPartition(logfile string, positin uint16) (string, uint16) {

}

type DefaultConsumer struct {
	*Config
	*sarama.PartitionConsumer
	MessageHandler
}

func NewDefaultConsumer(conf Config, handler MessageHandler) *BinlogConsumer {

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
