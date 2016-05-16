package producer

import (
	"encoding/json"
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
	*sarama.SyncProducer
	TopicProvider
	KeyProvider
	ResultHander
	ErrorHander
	*replication.BinlogSyncer
}

func NewBinlogProducer(conf *Config, syncProducer *sarama.SyncProducer) *BinlogProducer {
	pro := &BinlogProducer{}
	pro.Config = conf
	pro.initProducer()
	pro.InitBinlogSyncer()
	return pro
}

func (pro *BinlogProducer) SendMessage(
	ev *replication.BinlogEvent,
) (*EventWrapper, int32, int64, error) {
	_, ok := ev.Event.(*replication.RowsEvent)
	if !ok {
		return nil, 0, 0, nil
	}
	ev := NewBinlogEvent(ev)
	if ok := pro.EventFilter.Filtering(ev); !ok {
		return nil, 0, 0, nil
	}
	dat, err := json.Marshal(ev)
	if err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	msg := &sarama.ProducerMessage{
		Topic: pro.TopicProvider.Provide(ev),
		Key:   sarama.StringEncoder(pro.KeyProvier.Provide(ev)),
		Value: sarama.ByteEncoder(dat),
	}
	partition, offset, err := pro.SendMessage(msg)
	return ev, partition, offset, err
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
	Handle(*BinlogEvent, int, uint32)
}

type ErrorHandler interface {
	Handle(error) error
}

type ErrGetEvent struct {
	Inner error
}

func (err ErrGetEvent) Error() string {
	return fmt.Printf("faild to get event: %#v", err.Inner)
}

func newErrGetEvent(err error) ErrGetEvent {
	return errors.Trace(ErrGetEvent{err})
}

type ErrSendMessage struct {
	Inner error
}

func (err ErrSendMessage) Error() string {
	return fmt.Printf("faild to send message: %#v", err.Inner)
}

func newErrSendMessage(err error) ErrGetEvent {
	return errors.Trace(ErrSendMessage{err})
}
