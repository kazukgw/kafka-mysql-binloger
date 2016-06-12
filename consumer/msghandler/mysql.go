package msghandler

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/Shopify/sarama"
	"github.com/kazukgw/kafka-mysql-binloger/producer"
)

type MySQL struct {
	MySQLConfig
	sarama.Consumer
}

type MySQLConfig struct {
	Host     string
	Port     uint16
	User     string
	Password string
	Tables   []Table
}

func (con Mysql) InsertRows(msg *BinlogConsumerMessage) error {
	sqlstr, args, err := con.InsertSql(msg)
	if err != nil {
		return err
	}

}

func (con Mysql) InsertSql(msg *BinlogConsumerMessage) error {
	builder := sq.Insert().Columns()
	for _, r := range msg.Rows {
		builder = builder.Values()
	}
	return builder.ToSql()
}

func (con Mysql) UpdateRows(msg *BinlogConsumerMessage) error {
	for _, r := range msg.Rows {
		sqlstr, args, err := con.InsertSql(msg)
		if err != nil {
			return err
		}
	}
}

func (con Mysql) DeleteRows(msg *BinlogConsumerMessage) error {
	for _, r := range msg.Rows {
		sqlstr, args, err := con.InsertSql(msg)
		if err != nil {
			return err
		}
	}
}
