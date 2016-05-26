package consumer

import (
	"github.com/Shopify/sarama"
	"github.com/kazukgw/kafka-mysql-binloger/producer"
)

type Mysql struct {
	MysqlConfig
	sarama.Consumer
}

type MysqlConfig struct {
	Host     string
	Port     uint16
	User     string
	Password string
}

func (con Mysql) syncDBWithMsg(msg *sarama.ConsumerMessage) {
	event := &producer.BinlogEvent{}
	if err := json.Unmarshal(msg.Value, event); err != nil {
		panic(err.Error())
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

func (con Mysql) insertRows(
	tableMap map[string]interface{},
	rows []interface{},
	tableDefMap map[string]string,
) error {
	sqlstr := fmt.Sprintf("INSERT INTO %s ()")
}

func (con Mysql) updateRows(rows []interface{}) error {

}

func (con Mysql) deleteRows(rows []interface{}) error {

}
