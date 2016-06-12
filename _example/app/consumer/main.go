package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	sq "github.com/Masterminds/squirrel"
	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/kazukgw/kafka-mysql-binloger/producer"
)

func main() {
	dns := fmt.Sprintf(
		"%s:%s@tcp(%s)/appdb",
		os.Getenv("FOLLOWER_MYSQL_USER"),
		os.Getenv("FOLLOWER_MYSQL_PASSWORD"),
		os.Getenv("FOLLOWER_MYSQL_ADDR")+":"+os.Getenv("FOLLOWER_MYSQL_PORT"),
	)
	db, err := sql.Open("mysql", dns)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	brokers := []string{
		os.Getenv("KAFKA_BROKER_ADDR"),
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err.Error())
	}
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	topic := os.Getenv("KAFKA_BINLOG_TOPIC")
	_partition, _ := strconv.Atoi(os.Getenv("KAFKA_BINLOG_PARTITION"))
	partition := int32(_partition)

	con, err := master.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	msgHandler := MessageHandler{db}

	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-con.Errors():
				fmt.Println(err)
			case msg := <-con.Messages():
				msgHandler.HandleMessage(msg)
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
}

type MessageHandler struct {
	DB *sql.DB
}

func (handler *MessageHandler) HandleMessage(msg *sarama.ConsumerMessage) {
	event := &producer.BinlogEvent{}
	if err := json.Unmarshal(msg.Value, event); err != nil {
		panic(err.Error())
	}
	handler.HandleBinlogEvent(event)
}

func (handler *MessageHandler) HandleBinlogEvent(event *producer.BinlogEvent) {
	header := event.Header
	switch header.EventType {
	case producer.EVENT_TYPE_WRITE_ROWS_EVENTv0,
		producer.EVENT_TYPE_WRITE_ROWS_EVENTv1,
		producer.EVENT_TYPE_WRITE_ROWS_EVENTv2:
		rowsHandler := NewRowsEventHandler(handler, defaultMappings, event)
		rowsHandler.HandleWriteRowsEvent()
		return
	case producer.EVENT_TYPE_UPDATE_ROWS_EVENTv0,
		producer.EVENT_TYPE_UPDATE_ROWS_EVENTv1,
		producer.EVENT_TYPE_UPDATE_ROWS_EVENTv2:
		rowsHandler := NewRowsEventHandler(handler, defaultMappings, event)
		rowsHandler.HandleUpdateRowsEvent()
		return
	case producer.EVENT_TYPE_DELETE_ROWS_EVENTv0,
		producer.EVENT_TYPE_DELETE_ROWS_EVENTv1,
		producer.EVENT_TYPE_DELETE_ROWS_EVENTv2:
		rowsHandler := NewRowsEventHandler(handler, defaultMappings, event)
		rowsHandler.HandleDeleteRowsEvent()
		return
	default:
		log.Printf("==> event-type: %#v", header.EventType)
	}
}

type ColumnMapping struct {
	From   string
	To     string
	ToType string
}

type Mapping struct {
	FromTable      string
	ToTable        string
	ColumnMappings []ColumnMapping
}

var defaultMappings = []Mapping{
	{
		"users",
		"users",
		[]ColumnMapping{
			{"user_id", "user_id", ""},
			{"email", "email", ""},
			{"user_name", "user_name", ""},
		},
	},
	{
		"books",
		"books",
		[]ColumnMapping{
			{"book_id", "book_id", ""},
			{"ISBN", "ISBN", ""},
			{"author", "author", ""},
		},
	},
	{
		"tags",
		"tags",
		[]ColumnMapping{
			{"tag_id", "tag_id", ""},
			{"tag_name", "tag_name", ""},
			{"user_id", "user_id", ""},
			{"book_id", "book_id", ""},
		},
	},
}

type RowsEventHandler struct {
	*MessageHandler
	Mappings  []Mapping
	RowsEvent struct {
		Header    *producer.BinlogEventHeader
		Event     map[string]interface{}
		TableMap  map[string]interface{}
		TableName string
		Rows      []interface{}
	}
}

func NewRowsEventHandler(
	h *MessageHandler,
	mappings []Mapping,
	event *producer.BinlogEvent,
) *RowsEventHandler {
	ev := event.Event.(map[string]interface{})
	tmap := ev["Table"].(map[string]interface{})
	rows := ev["Rows"].([]interface{})
	tableName := tmap["Table"].(string)
	return &RowsEventHandler{
		MessageHandler: h,
		Mappings:       mappings,
		RowsEvent: struct {
			Header    *producer.BinlogEventHeader
			Event     map[string]interface{}
			TableMap  map[string]interface{}
			TableName string
			Rows      []interface{}
		}{event.Header, ev, tmap, tableName, rows},
	}
}

func (handler *RowsEventHandler) MappedTable(tableName string) string {
	for _, tmap := range handler.Mappings {
		if tmap.FromTable == tableName {
			return tmap.ToTable
		}
	}
	return ""
}

func (handler *RowsEventHandler) MappedColumns(tableName string) []string {
	cols := []string{}
	for _, tmap := range handler.Mappings {
		if tmap.FromTable == tableName {
			for _, cm := range tmap.ColumnMappings {
				cols = append(cols, cm.To)
			}
			break
		}
	}
	return cols
}

func (handler *RowsEventHandler) HandleWriteRowsEvent() {
	mappedTable := handler.MappedTable(handler.RowsEvent.TableName)
	cols := handler.MappedColumns(handler.RowsEvent.TableName)

	builder := sq.Insert(mappedTable).Columns(cols...)
	for _, row := range handler.RowsEvent.Rows {
		r, _ := row.([]interface{})
		builder.Values(r...)
	}
	sqlstr, args, err := builder.ToSql()
	if err != nil {
		panic(err)
	}

	_, err = handler.MessageHandler.DB.Exec(sqlstr, args...)
	if err != nil {
		panic(err)
	}
}

func (handler *RowsEventHandler) HandleUpdateRowsEvent() {
	mappedTable := handler.MappedTable(handler.RowsEvent.TableName)
	cols := handler.MappedColumns(handler.RowsEvent.TableName)
	builder := sq.Update(mappedTable)

	for i := 0; i < len(handler.RowsEvent.Rows); i += 2 {
		b := builder
		identification, _ := handler.RowsEvent.Rows[i].([]interface{})
		r, _ := handler.RowsEvent.Rows[i+1].([]interface{})
		for colIdx, v := range r {
			if v != nil {
				b = b.Set(cols[colIdx], v)
			}
		}
		eq := sq.Eq{}
		for colIdx, v := range identification {
			eq[cols[colIdx]] = v
		}
		b = b.Where(eq)
		sqlstr, args, err := b.ToSql()
		if err != nil {
			panic(err)
		}
		_, err = handler.MessageHandler.DB.Exec(sqlstr, args)
		if err != nil {
			panic(err)
		}
	}
}

func (handler *RowsEventHandler) HandleDeleteRowsEvent() {
	mappedTable := handler.MappedTable(handler.RowsEvent.TableName)
	cols := handler.MappedColumns(handler.RowsEvent.TableName)
	builder := sq.Delete(mappedTable)

	for _, row := range handler.RowsEvent.Rows {
		b := builder
		identification, _ := row.([]interface{})
		eq := sq.Eq{}
		for colIdx, v := range identification {
			eq[cols[colIdx]] = v
		}
		b = b.Where(eq)
		sqlstr, args, err := b.ToSql()
		if err != nil {
			panic(err)
		}
		_, err = handler.MessageHandler.DB.Exec(sqlstr, args)
		if err != nil {
			panic(err)
		}
	}
}
