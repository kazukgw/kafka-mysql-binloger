package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	dns := fmt.Sprintf(
		"%s:%s@tcp(%s)/appdb",
		os.Getenv("FOLLOWER_MYSQL_USER"),
		os.Getenv("FOLLOWER_MYSQL_PASSWORD"),
		os.Getenv("FOLLOWER_MYSQL_ADDR"),
	)
	db, err := sql.Open("mysql", dns)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	brokers := []string{
		os.Getenv("KAFKA_BROKER_ADDR"),
	}

	con := &BinlogConsumerMaster{}
	con.BinlogConsumerMasterConfig = conf
	master, err := sarama.NewConsumer(conf.Sarama.Brokers, conf.Sarama.Config)

	log.Println("==> 2")
	master, err := NewBinlogConsumerMaster(masterConf)
	if err != nil {
		panic(err.Error())
	}
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()
	log.Println("==> 3")
	config := NewBinlogConsumerConfig()
	con, err := master.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	log.Println("==> 4")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-con.Errors():
				fmt.Println(err)
			case msg := <-con.Messages():
				updateWithMsg(db, msg)
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
}

func updateWithMsg(db *sql.DB, msg *sarama.ConsumerMessage) {
	log.Println("-------------")
	event := &EventForMarshalling{}
	if err := json.Unmarshal(msg.Value, event); err != nil {
		panic(err.Error())
	}
	log.Println(event.Header.EventType)
	ev := event.Event.(map[string]interface{})
	tmap := ev["Table"].(map[string]interface{})
	rows := ev["Rows"].([]interface{})
	log.Println(tmap["Table"])
	log.Println(ev["Rows"])
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
