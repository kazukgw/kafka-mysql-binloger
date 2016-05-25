package main

import (
	"log"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/kazukgw/kafka-mysql-binloger/producer"
)

func main() {
	brokers := []string{
		os.Getenv("KAFKA_BROKER_ADDR"),
	}
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Retry.Max = 5
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	syncProducer, err := sarama.NewSyncProducer(brokers, saramaConfig)
	if err != nil {
		panic(err.Error())
	}

	conf := &producer.Config{}
	conf.Mysql.Host = os.Getenv("MYSQL_ADDR")
	portnum, err := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if err != nil {
		panic(err.Error())
	}
	conf.Mysql.Port = uint16(portnum)
	conf.Mysql.User = os.Getenv("MYSQL_USER")
	conf.Mysql.Password = os.Getenv("MYSQL_PASSWORD")
	conf.BinlogSyncer.File = os.Getenv("BINLOGSYNCER_FILE")
	offsetnum, err := strconv.Atoi(os.Getenv("BINLOGSYNCER_OFFSET"))
	if err != nil {
		panic(err.Error())
	}
	conf.BinlogSyncer.Offset = uint32(offsetnum)

	binlogProducer := producer.NewBinlogProducer(conf, syncProducer)

	binlogProducer.TopicProvider = topicProvider{}
	binlogProducer.KeyProvider = keyProvider{}
	binlogProducer.ResultHandler = resultHandler{}
	binlogProducer.ErrorHandler = errorHandler{}

	if err := binlogProducer.Start(); err != nil {
		panic(err.Error())
	}
}

type topicProvider struct{}

func (p topicProvider) Provide(ev *producer.BinlogEvent) string {
	return "mysql-binlog-test"
}

type keyProvider struct{}

func (p keyProvider) Provide(ev *producer.BinlogEvent) string {
	return "a-key"
}

type resultHandler struct{}

func (h resultHandler) Handle(ev *producer.BinlogEvent, partition int32, offset int64) {
	if ev == nil {
		return
	}
	log.Printf("==> event: %#v", ev)
}

type errorHandler struct{}

func (eh errorHandler) Handle(err error) error {
	log.Printf("==> err: %#v", err.Error())
	return nil
}
