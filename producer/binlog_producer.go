package producer

import (
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

// todo implement Config

type BinlogProducer struct {
	*Config
	*sarama.SyncProducer
	*replication.BinlogSyncer
}

func NewBinlogProducer(conf *Config, syncProducer *sarama.SyncProducer) *BinlogProducer {
	pro := &BinlogProducer{}
	pro.Config = conf
	pro.initProducer()
	pro.InitBinlogSyncer()
	return pro
}

func (pro *BinlogProducer) initBinlogSyncer() {
	// todo use config
	pro.BinlogSyncer = replication.NewBinlogSyncer(100, "mysql")
}

func (pro *BinlogProducer) initProducer() {
	producer, err := sarama.NewSyncProducer(
		// todo modify config
		pro.Config.Sarama.Brokers,
		pro.Config.Sarama.Config,
	)
	if err != nil {
		return errors.Trace(err)
	}
	pro.SyncProducer = producer
}

func (pro *BinlogProducer) SendMessage(
	ev *replication.BinlogEvent,
) (*EventWrapper, int32, int64, error) {
	_, ok := ev.Event.(*replication.RowsEvent)
	if !ok {
		return nil, 0, 0, nil
	}
	// todo implement NewEventWrapper(ev)
	evWrapper := NewEventWrapper(ev)
	// todo implement EventFilter
	if ok := pro.EventFilter.Filter(evWrapper); !ok {
		return nil, 0, 0, nil
	}
	dat, err := json.Marshal(evWrapper)
	if err != nil {
		return nil, 0, 0, errors.Trace(err)
	}
	msg := &sarama.ProducerMessage{
		// todo implement TopicProvider
		Topic: pro.TopicProvider.Provide(evWrapper),
		// todo implement KeyProvider
		Key:   sarama.StringEncoder(pro.KeyProvier.Provide(evWrapper)),
		Value: sarama.ByteEncoder(dat),
	}
	partition, offset, err := pro.SendMessage(msg)
	return evWrapper, partition, offset, err
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
		pro.Config.Mysql.File,
		pro.Config.Mysql.Offset,
	}

	streamer, err := pro.BinlogSyncer.StartSync(pos)
	if err != nil {
		return errors.Trace(err)
	}

	for {
		ev, err := streamer.GetEvent()
		if err != nil {
			// todo implement ErrorHandler
			pro.ErrorHandler.Handle(newErrGetEvent(err))
			continue
		}
		evWrapper, partition, offset, err := pro.SendMessage(ev)
		if err != nil {
			pro.ErrorHandler.Handle(newErrSendMessage(err))
			continue
		}
		// todo implement ResultHandler
		pro.ResultHandler.Handle(evWrapper, partition, offset)
	}
	return nil
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
