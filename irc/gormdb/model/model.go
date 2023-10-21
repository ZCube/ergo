package model

import (
	"gorm.io/datatypes"
)

type Metadata struct {
	KeyName string `gorm:"primary_key;type:varchar(32);column:key_name"`
	Value   string `gorm:"not null;type:varchar(32)"`
}

func (h *Metadata) TableName() string {
	return "metadata"
}

type History struct {
	ID    int64          `gorm:"primary_key;auto_increment;column:id"`
	Data  datatypes.JSON `gorm:"not null;type:blob;column:data"`
	MsgID string         `gorm:"not null;type:varchar(26);column:msgid"`
}

func (h *History) TableName() string {
	return "history"
}

type Sequence struct {
	HistoryID int64  `gorm:"primary_key;column:history_id"`
	Target    string `gorm:"not null;type:varchar(64);column:target"`
	Nanotime  uint64 `gorm:"not null;column:nanotime"`
}

func (h *Sequence) TableName() string {
	return "sequence"
}

type Conversations struct {
	ID            int64  `gorm:"primary_key;auto_increment;column:id"`
	Target        string `gorm:"not null;type:varchar(64);column:target"`
	Correspondent string `gorm:"not null;type:varchar(64);column:correspondent"`
	Nanotime      uint64 `gorm:"not null;column:nanotime"`
	HistoryID     uint64 `gorm:"not null;column:history_id"`
}

func (h *Conversations) TableName() string {
	return "conversations"
}

type Correspondents struct {
	ID            int64  `gorm:"primary_key;auto_increment;column:id"`
	Target        string `gorm:"not null;type:varchar(64);column:target"`
	Correspondent string `gorm:"not null;type:varchar(64);column:correspondent"`
	Nanotime      uint64 `gorm:"not null;column:nanotime"`
}

func (h *Correspondents) TableName() string {
	return "correspondents"
}

type AccountMessages struct {
	HistoryID int64  `gorm:"primary_key;column:history_id"`
	Account   string `gorm:"not null;type:varchar(64);column:account"`
}

func (h *AccountMessages) TableName() string {
	return "account_messages"
}

type Forget struct {
	ID      int64  `gorm:"primary_key;auto_increment;column:id"`
	Account string `gorm:"not null;type:varchar(64);column:account"`
}

func (h *Forget) TableName() string {
	return "forget"
}
