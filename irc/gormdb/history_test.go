// Copyright (c) 2018 Shivaram Lingamneni <slingamn@cs.stanford.edu>
// released under the MIT license

package gormdb

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/ergochat/ergo/irc/history"
	"github.com/ergochat/ergo/irc/logger"
	"github.com/ergochat/ergo/irc/utils"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestHistory(t *testing.T) {
	Gorm(t, "mysql", "mariadb", "mariadb:mariadb@tcp(localhost:43306)/mariadb?charset=utf8mb4&parseTime=True&loc=Local")
	Gorm(t, "postgres", "postgres", "host=localhost user=postgres password=postgres dbname=postgres port=45432 sslmode=disable TimeZone=Asia/Seoul")
}

func Gorm(t *testing.T, driver string, database string, dsn string) {
	var db *gorm.DB
	var err error
	switch driver {
	case "mysql":
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
			PrepareStmt: true,
		})
	case "postgres":
		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
			PrepareStmt: true,
		})
	}
	if err != nil {
		fmt.Printf("Error opening history database: %s %s", err.Error(), driver)
		t.Error(err.Error())
		return
	}

	for _, table := range []string{"metadata", "history", "sequence", "conversations", "correspondents", "account_messages", "forget"} {
		err = db.Exec("DROP TABLE IF EXISTS " + table).Error
		if err != nil {
			fmt.Printf("Error delete history database: %s %s", err.Error(), driver)
			t.Error(err.Error())
			return
		}
	}
	db = nil

	var logger logger.Manager
	g := GormDB{}
	config := Config{
		Enabled:         true,
		DSN:             dsn,
		Driver:          driver,
		HistoryDatabase: database,
		Debug:           true,
		Timeout:         time.Second * 5,
	}
	g.Initialize(&logger, config)
	g.SetConfig(config)
	var historyDB history.HistoryInterface
	historyDB = &g
	err = historyDB.Open()
	if err != nil {
		fmt.Printf("Error opening history database: %s %s", err.Error(), driver)
		t.Error(err.Error())
		return
	}
	defer historyDB.Close()
	err = historyDB.AddChannelItem("test", history.Item{
		Type: history.Part,
		Nick: "test",
		// this is the uncasefolded account name, if there's no account it should be set to "*"
		AccountName: "test",
		// for non-privmsg items, we may stuff some other data in here
		Message:         utils.MakeMessage("asdfasdfasdfasdf"),
		Tags:            map[string]string{"asdf": "asdf"},
		CfCorrespondent: "",
		IsBot:           false,
	}, "test2")
	if err != nil {
		fmt.Printf("Error : %s %s", err.Error(), driver)
		t.Error(err.Error())
		return
	}
	err = historyDB.AddChannelItem("test3", history.Item{
		Type: history.Topic,
		Nick: "test",
		// this is the uncasefolded account name, if there's no account it should be set to "*"
		AccountName: "test",
		// for non-privmsg items, we may stuff some other data in here
		Message:         utils.MakeMessage("asdfasdfasdfasdf"),
		Tags:            map[string]string{"asdf": "asdf"},
		CfCorrespondent: "",
		IsBot:           false,
	}, "test2")
	if err != nil {
		fmt.Printf("Error : %s %s", err.Error(), driver)
		t.Error(err.Error())
		return
	}
	err = historyDB.AddDirectMessage("from", "fromAccount", "to", "toAccount", history.Item{
		Type: history.Topic,
		Nick: "test",
		// this is the uncasefolded account name, if there's no account it should be set to "*"
		AccountName: "fromAccount",
		// for non-privmsg items, we may stuff some other data in here
		Message:         utils.MakeMessage("asdfasdfasdfasdf"),
		Tags:            map[string]string{"asdf": "asdf"},
		CfCorrespondent: "",
		IsBot:           false,
	})
	if err != nil {
		fmt.Printf("Error : %s %s", err.Error(), driver)
		t.Error(err.Error())
		return
	}
	targetListing, err := historyDB.ListChannels([]string{"test", "test2", "test3"})
	if err != nil {
		fmt.Printf("Error : %s %s", err.Error(), driver)
		t.Error(err.Error())
		return
	}
	if err != nil {
		fmt.Printf("Error : %s %s", err.Error(), driver)
		t.Error(err.Error())
		return
	}
	fmt.Println(targetListing)
	sequence := historyDB.MakeSequence("fromAccount", "to", time.Now())
	t0 := time.Now().UTC()
	t1 := time.Now().UTC().Round(0)
	t2 := time.Now().UTC().Round(0).Add(-time.Hour)
	t3 := time.Now().UTC().Round(0).Add(+time.Hour)
	fmt.Println(t0.UnixNano(), t1.UnixNano(), t2.UnixNano(), t3.UnixNano())
	start := history.Selector{
		Time: time.Now().UTC().Round(0).Add(-time.Hour),
	}
	end := history.Selector{
		Time: time.Now().UTC().Round(0).Add(time.Hour),
	}
	targetListing, err = sequence.ListCorrespondents(start, end, 100)
	if err != nil {
		fmt.Printf("Error : %s %s", err.Error(), driver)
		t.Error(err.Error())
		return
	}
	if err != nil {
		fmt.Printf("Error : %s %s", err.Error(), driver)
		t.Error(err.Error())
		return
	}
	items, err := sequence.Between(history.Selector{}, history.Selector{}, 100)
	if err != nil {
		fmt.Printf("Error : %s %s", err.Error(), driver)
		t.Error(err.Error())
		return
	}
	fmt.Println(items)
	fmt.Println(targetListing)
	fmt.Println(sequence)
	buf := bytes.NewBuffer(nil)
	historyDB.Export("test", buf)
	historyDB.Forget("test")
	fmt.Println(buf.String())
}
