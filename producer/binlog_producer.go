package producer

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type Config struct {
	Mysql struct {
		Host     string
		Port     uint16
		User     string
		Password string
	}
	BinlogSyncer struct {
		File   string
		Offset uint32
	}
}

type BinlogProducer struct {
	*Config
	sarama.SyncProducer
	EventFilter
	TopicProvider
	KeyProvider
	ResultHandler
	ErrorHandler
	*replication.BinlogSyncer
}

func NewBinlogProducer(conf *Config, syncProducer sarama.SyncProducer) *BinlogProducer {
	pro := &BinlogProducer{}
	pro.BinlogSyncer = replication.NewBinlogSyncer(100, "mysql")
	pro.SyncProducer = syncProducer
	pro.Config = conf
	return pro
}

func (pro *BinlogProducer) SendMessage(
	ev *replication.BinlogEvent,
) (*BinlogEvent, int32, int64, error) {
	_, ok := ev.Event.(*replication.RowsEvent)
	if !ok {
		return nil, 0, 0, nil
	}
	binev := NewBinlogEvent(ev)
	if pro.EventFilter != nil {
		if ok := pro.EventFilter.Filtering(binev); !ok {
			return nil, 0, 0, nil
		}
	}
	dat, err := json.Marshal(binev)
	if err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	msg := &sarama.ProducerMessage{
		Topic: pro.TopicProvider.Provide(binev),
		Key:   sarama.StringEncoder(pro.KeyProvider.Provide(binev)),
		Value: sarama.ByteEncoder(dat),
	}
	partition, offset, err := pro.SyncProducer.SendMessage(msg)
	return binev, partition, offset, err
}

func (pro *BinlogProducer) Start() error {
	err := pro.BinlogSyncer.RegisterSlave(
		pro.Config.Mysql.Host,
		pro.Config.Mysql.Port,
		pro.Config.Mysql.User,
		pro.Config.Mysql.Password,
	)
	if err != nil {
		return errors.Trace(err)
	}
	pos := mysql.Position{
		pro.Config.BinlogSyncer.File,
		pro.Config.BinlogSyncer.Offset,
	}

	streamer, err := pro.BinlogSyncer.StartSync(pos)
	if err != nil {
		return errors.Trace(err)
	}

	for {
		ev, err := streamer.GetEvent()
		log.Println("==> get event")
		if err != nil {
			if err := pro.ErrorHandler.Handle(newErrGetEvent(err)); err != nil {
				break
			}
			continue
		}
		binlogEv, partition, offset, err := pro.SendMessage(ev)
		if err != nil {
			if err := pro.ErrorHandler.Handle(newErrSendMessage(err)); err != nil {
				break
			}
			continue
		}
		pro.ResultHandler.Handle(binlogEv, partition, offset)
	}
	return nil
}

type EventFilter interface {
	Filtering(*BinlogEvent) bool
}

type TopicProvider interface {
	Provide(*BinlogEvent) string
}

type KeyProvider interface {
	Provide(*BinlogEvent) string
}

type ResultHandler interface {
	Handle(*BinlogEvent, int32, int64)
}

type ErrorHandler interface {
	Handle(error) error
}

type ErrGetEvent struct {
	Inner error
}

func (err ErrGetEvent) Error() string {
	return fmt.Sprintf("faild to get event: %#v", err.Inner)
}

func newErrGetEvent(err error) error {
	return errors.Trace(ErrGetEvent{err})
}

type ErrSendMessage struct {
	Inner error
}

func (err ErrSendMessage) Error() string {
	return fmt.Sprintf("faild to send message: %#v", err.Inner)
}

func newErrSendMessage(err error) error {
	return errors.Trace(ErrSendMessage{err})
}
