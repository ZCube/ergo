package gormdb

type Metadata struct {
	KeyName string `gorm:"primary_key;type:varchar(32);column:key_name"`
	Value   string `gorm:"not null;type:varchar(32)"`
}

// TableName sets the table name for the Metadata struct
func (m *Metadata) TableName() string {
	return "metadata"
}

type History struct {
	ID    uint64   `gorm:"primary_key;auto_increment;column:id"`
	Data  []byte   `gorm:"not null;type:blob;column:data"`
	MsgID [16]byte `gorm:"not null;type:binary(16);column:msgid"`
}

// TableName sets the table name for the History struct
func (h *History) TableName() string {
	return "history"
}
