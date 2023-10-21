// Copyright (c) 2020 Shivaram Lingamneni
// released under the MIT license

package gormdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ergochat/ergo/irc/gormdb/model"
	"github.com/ergochat/ergo/irc/history"
	"github.com/ergochat/ergo/irc/logger"
	"github.com/ergochat/ergo/irc/utils"
	"gorm.io/datatypes"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	ErrDisallowed = errors.New("disallowed")
)

const (
	// maximum length in bytes of any message target (nickname or channel name) in its
	// canonicalized (i.e., casefolded) state:
	MaxTargetLength = 64

	// latest schema of the db
	latestDbSchema   = "2"
	keySchemaVersion = "db.version"
	// minor version indicates rollback-safe upgrades, i.e.,
	// you can downgrade oragono and everything will work
	latestDbMinorVersion  = "2"
	keySchemaMinorVersion = "db.minorversion"
	cleanupRowLimit       = 50
	cleanupPauseTime      = 10 * time.Minute
)

type e struct{}

type GormDB struct {
	db     *gorm.DB
	logger *logger.Manager

	insertHistory        string
	insertSequence       string
	insertConversation   string
	insertCorrespondent  string
	insertAccountMessage string

	stateMutex sync.Mutex
	config     Config

	wakeForgetter chan e

	timeout              atomic.Uint64
	trackAccountMessages atomic.Uint32
}

func (gormDB *GormDB) Initialize(logger *logger.Manager, config Config) {
	gormDB.logger = logger
	gormDB.wakeForgetter = make(chan e, 1)
	gormDB.SetConfig(config)
}

func (gormDB *GormDB) SetConfig(config Config) {
	gormDB.timeout.Store(uint64(config.Timeout))
	var trackAccountMessages uint32
	if config.TrackAccountMessages {
		trackAccountMessages = 1
	}
	gormDB.trackAccountMessages.Store(trackAccountMessages)
	gormDB.stateMutex.Lock()
	gormDB.config = config
	gormDB.stateMutex.Unlock()
}

func (gormDB *GormDB) getExpireTime() (expireTime time.Duration) {
	gormDB.stateMutex.Lock()
	expireTime = gormDB.config.ExpireTime
	gormDB.stateMutex.Unlock()
	return
}

func (m *GormDB) Open() (err error) {
	switch m.config.Driver {
	case "mysql":
		m.db, err = gorm.Open(mysql.Open(m.config.DSN), &gorm.Config{
			PrepareStmt: false,
		})
	case "postgres":
		m.db, err = gorm.Open(postgres.Open(m.config.DSN), &gorm.Config{
			PrepareStmt: false,
		})
	default:
		return fmt.Errorf("unknown driver %s", m.config.Driver)
	}
	if err != nil {
		return err
	}

	if m.config.Debug {
		m.db = m.db.Debug()
	}

	sqlDB, err := m.db.DB()
	if err != nil {
		return err
	}

	if m.config.MaxConns != 0 {
		sqlDB.SetMaxOpenConns(m.config.MaxConns)
		sqlDB.SetMaxIdleConns(m.config.MaxConns)
	}
	if m.config.ConnMaxLifetime != 0 {
		sqlDB.SetConnMaxLifetime(m.config.ConnMaxLifetime)
	}

	err = m.fixSchemas()
	if err != nil {
		return err
	}

	err = m.prepareStatements()
	if err != nil {
		return err
	}

	go m.cleanupLoop()
	go m.forgetLoop()

	return nil
}

func (gormDB *GormDB) fixSchemas() (err error) {
	switch gormDB.config.Driver {
	case "mysql":
		err = gormDB.db.Exec(`CREATE TABLE IF NOT EXISTS metadata (
			key_name VARCHAR(32) primary key,
			value VARCHAR(32) NOT NULL
		) CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`).Error
		if err != nil {
			return err
		}
	case "postgres":
		err = gormDB.db.Exec(`CREATE EXTENSION IF NOT EXISTS pg_trgm;`).Error
		if err != nil {
			return err
		}
		err = gormDB.db.Exec(`CREATE TABLE IF NOT EXISTS metadata (
			key_name VARCHAR(32) primary key,
			value VARCHAR(32) NOT NULL
		);`).Error
		if err != nil {
			return err
		}
	}

	schema := model.Metadata{}
	err = gormDB.db.Raw(`select value from metadata where key_name = ?;`, keySchemaVersion).First(&schema).Error
	if err == gorm.ErrRecordNotFound {
		err = gormDB.createTables()
		if err != nil {
			return
		}
		err = gormDB.db.Exec(`insert into metadata (key_name, value) values (?, ?);`, keySchemaVersion, latestDbSchema).Error
		if err != nil {
			return
		}
		err = gormDB.db.Exec(`insert into metadata (key_name, value) values (?, ?);`, keySchemaMinorVersion, latestDbMinorVersion).Error
		if err != nil {
			return
		}
		return
	} else if err == nil && schema.Value != latestDbSchema {
		// TODO figure out what to do about schema changes
		return fmt.Errorf("incompatible schema: got %s, expected %s", schema, latestDbSchema)
	} else if err != nil {
		return err
	}

	minorVersion := model.Metadata{}
	err = gormDB.db.Raw(`select value from metadata where key_name = ?;`, keySchemaMinorVersion).First(&minorVersion).Error
	if err == gorm.ErrRecordNotFound {
		// XXX for now, the only minor version upgrade is the account tracking tables
		err = gormDB.createComplianceTables()
		if err != nil {
			return
		}
		err = gormDB.createCorrespondentsTable()
		if err != nil {
			return
		}
		err = gormDB.db.Exec(`insert into metadata (key_name, value) values (?, ?);`, keySchemaMinorVersion, latestDbMinorVersion).Error
		if err != nil {
			return
		}
	} else if err == nil && minorVersion.Value == "1" {
		// upgrade from 2.1 to 2.2: create the correspondents table
		err = gormDB.createCorrespondentsTable()
		if err != nil {
			return
		}
		err = gormDB.db.Exec(`update metadata set value = ? where key_name = ?;`, latestDbMinorVersion, keySchemaMinorVersion).Error
		if err != nil {
			return
		}
	} else if err == nil && minorVersion.Value != latestDbMinorVersion {
		// TODO: if minorVersion < latestDbMinorVersion, upgrade,
		// if latestDbMinorVersion < minorVersion, ignore because backwards compatible
	}
	return
}

func (gormDB *GormDB) createTables() (err error) {
	tx := gormDB.db.Session(&gorm.Session{PrepareStmt: false})
	switch gormDB.config.Driver {
	case "mysql":
		err = tx.Exec(`CREATE TABLE history (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			data BLOB NOT NULL,
			msgid VARCHAR(26) NOT NULL,
			KEY (msgid(4))
		) CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`).Error
		if err != nil {
			return err
		}

		err = tx.Exec(fmt.Sprintf(`CREATE TABLE sequence (
			history_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
			target VARBINARY(%[1]d) NOT NULL,
			nanotime BIGINT UNSIGNED NOT NULL,
			KEY (target, nanotime)
		) CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`, MaxTargetLength)).Error
		if err != nil {
			return err
		}
		/* XXX: this table used to be:
		CREATE TABLE sequence (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			target VARBINARY(%[1]d) NOT NULL,
			nanotime BIGINT UNSIGNED NOT NULL,
			history_id BIGINT NOT NULL,
			KEY (target, nanotime),
			KEY (history_id)
		) CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
		Some users may still be using the old schema.
		*/

		err = tx.Exec(fmt.Sprintf(`CREATE TABLE conversations (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			target VARBINARY(%[1]d) NOT NULL,
			correspondent VARBINARY(%[1]d) NOT NULL,
			nanotime BIGINT UNSIGNED NOT NULL,
			history_id BIGINT NOT NULL,
			KEY (target, correspondent, nanotime),
			KEY (history_id)
		) CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`, MaxTargetLength)).Error
		if err != nil {
			return err
		}
	case "postgres":
		err = tx.Exec(`CREATE TABLE history (
			id bigserial PRIMARY KEY,
			data JSONB NOT NULL,
			msgid VARCHAR(26) NOT NULL
		);
		CREATE INDEX idx_msgid_prefix ON history USING GIST (msgid gist_trgm_ops);
		`).Error
		if err != nil {
			return err
		}

		err = tx.Exec(`CREATE TABLE sequence (
			history_id BIGINT NOT NULL PRIMARY KEY,
			target BYTEA NOT NULL,
			nanotime BIGINT NOT NULL
		);
		CREATE INDEX idx_sequence_target_nanotime ON sequence (target, nanotime);
		`).Error
		if err != nil {
			return err
		}

		err = tx.Exec(`CREATE TABLE conversations (
			id bigserial PRIMARY KEY,
			target BYTEA NOT NULL,
			correspondent BYTEA NOT NULL,
			nanotime BIGINT NOT NULL,
			history_id BIGINT NOT NULL
		);
		CREATE INDEX idx_conversations_target_correspondent_nanotime ON conversations (target, correspondent, nanotime);
		CREATE INDEX idx_conversations_history_id ON conversations (history_id);
		`).Error
		if err != nil {
			return err
		}
	}

	err = gormDB.createCorrespondentsTable()
	if err != nil {
		return err
	}

	err = gormDB.createComplianceTables()
	if err != nil {
		return err
	}

	return nil
}

func (gormDB *GormDB) createCorrespondentsTable() (err error) {
	tx := gormDB.db.Session(&gorm.Session{PrepareStmt: false})
	switch gormDB.config.Driver {
	case "mysql":
		err = tx.Exec(fmt.Sprintf(`CREATE TABLE correspondents (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			target VARBINARY(%[1]d) NOT NULL,
			correspondent VARBINARY(%[1]d) NOT NULL,
			nanotime BIGINT UNSIGNED NOT NULL,
			UNIQUE KEY (target, correspondent),
			KEY (target, nanotime),
			KEY (nanotime)
		) CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`, MaxTargetLength)).Error
	case "postgres":
		err = tx.Exec(fmt.Sprintf(`CREATE TABLE correspondents (
			id bigserial PRIMARY KEY,
			target BYTEA NOT NULL,
			correspondent BYTEA NOT NULL,
			nanotime BIGINT NOT NULL,
			UNIQUE (target, correspondent)
			
		);
		CREATE INDEX idx_correspondents_target_nanotime ON correspondents (target, nanotime);
		CREATE INDEX idx_correspondents_nanotime ON correspondents (nanotime);
		`)).Error
	}
	return
}

func (gormDB *GormDB) createComplianceTables() (err error) {
	tx := gormDB.db.Session(&gorm.Session{PrepareStmt: false})
	switch gormDB.config.Driver {
	case "mysql":
		err = tx.Exec(fmt.Sprintf(`CREATE TABLE account_messages (
			history_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
			account VARBINARY(%[1]d) NOT NULL,
			KEY (account, history_id)
		) CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`, MaxTargetLength)).Error
		if err != nil {
			return err
		}

		err = tx.Exec(fmt.Sprintf(`CREATE TABLE forget (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			account VARBINARY(%[1]d) NOT NULL
		) CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`, MaxTargetLength)).Error
		if err != nil {
			return err
		}
	case "postgres":
		err = tx.Exec(fmt.Sprintf(`CREATE TABLE account_messages (
			history_id BIGINT NOT NULL PRIMARY KEY,
			account BYTEA NOT NULL
		);
		CREATE INDEX idx_account_messages_account_history_id ON account_messages (account, history_id);
		`)).Error
		if err != nil {
			return err
		}

		err = tx.Exec(fmt.Sprintf(`CREATE TABLE forget (
			id bigserial PRIMARY KEY,
			account BYTEA NOT NULL
		);`)).Error
		if err != nil {
			return err
		}
	}

	return nil
}

func (gormDB *GormDB) cleanupLoop() {
	defer func() {
		if r := recover(); r != nil {
			gormDB.logger.Error("gormDB",
				fmt.Sprintf("Panic in cleanup routine: %v\n%s", r, debug.Stack()))
			time.Sleep(cleanupPauseTime)
			go gormDB.cleanupLoop()
		}
	}()

	for {
		expireTime := gormDB.getExpireTime()
		if expireTime != 0 {
			for {
				startTime := time.Now()
				rowsDeleted, err := gormDB.doCleanup(expireTime)
				elapsed := time.Now().Sub(startTime)
				gormDB.logError("error during row cleanup", err)
				// keep going as long as we're accomplishing significant work
				// (don't busy-wait on small numbers of rows expiring):
				if rowsDeleted < (cleanupRowLimit / 10) {
					break
				}
				// crude backpressure mechanism: if the database is slow,
				// give it time to process other queries
				time.Sleep(elapsed)
			}
		}
		time.Sleep(cleanupPauseTime)
	}
}

func (gormDB *GormDB) doCleanup(age time.Duration) (count int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupPauseTime)
	defer cancel()

	ids, maxNanotime, err := gormDB.selectCleanupIDs(ctx, age)
	if len(ids) == 0 {
		gormDB.logger.Debug("gormDB", "found no rows to clean up")
		return
	}

	gormDB.logger.Debug("gormDB", fmt.Sprintf("deleting %d history rows, max age %s", len(ids), utils.NanoToTimestamp(maxNanotime)))

	if maxNanotime != 0 {
		gormDB.deleteCorrespondents(ctx, maxNanotime)
	}

	return len(ids), gormDB.deleteHistoryIDs(ctx, ids)
}

func (gormDB *GormDB) deleteHistoryIDs(ctx context.Context, ids []uint64) (err error) {
	// can't use ? binding for a variable number of arguments, build the IN clause manually
	var inBuf strings.Builder
	inBuf.WriteByte('(')
	for i, id := range ids {
		if i != 0 {
			inBuf.WriteRune(',')
		}
		fmt.Fprintf(&inBuf, "%d", id)
	}
	inBuf.WriteRune(')')
	inClause := inBuf.String()

	err = gormDB.db.WithContext(ctx).Exec(fmt.Sprintf(`DELETE FROM conversations WHERE history_id in %s;`, inClause)).Error
	if err != nil {
		return
	}
	err = gormDB.db.WithContext(ctx).Exec(fmt.Sprintf(`DELETE FROM sequence WHERE history_id in %s;`, inClause)).Error
	if err != nil {
		return
	}
	if gormDB.isTrackingAccountMessages() {
		err = gormDB.db.WithContext(ctx).Exec(fmt.Sprintf(`DELETE FROM account_messages WHERE history_id in %s;`, inClause)).Error
		if err != nil {
			return
		}
	}
	err = gormDB.db.WithContext(ctx).Exec(fmt.Sprintf(`DELETE FROM history WHERE id in %s;`, inClause)).Error
	if err != nil {
		return
	}

	return
}

func (gormDB *GormDB) selectCleanupIDs(ctx context.Context, age time.Duration) (ids []uint64, maxNanotime int64, err error) {
	rows, err := gormDB.db.WithContext(ctx).Raw(`
		SELECT history.id, sequence.nanotime, conversations.nanotime
		FROM history
		LEFT JOIN sequence ON history.id = sequence.history_id
		LEFT JOIN conversations on history.id = conversations.history_id
		ORDER BY history.id LIMIT ?;`, cleanupRowLimit).Rows()
	if err != nil {
		return
	}
	defer rows.Close()

	idset := make(map[uint64]struct{}, cleanupRowLimit)
	threshold := time.Now().Add(-age).UnixNano()
	for rows.Next() {
		var id uint64
		var seqNano, convNano sql.NullInt64
		err = rows.Scan(&id, &seqNano, &convNano)
		if err != nil {
			return
		}
		nanotime := extractNanotime(seqNano, convNano)
		// returns 0 if not found; in that case the data is inconsistent
		// and we should delete the entry
		if nanotime < threshold {
			idset[id] = struct{}{}
			if nanotime > maxNanotime {
				maxNanotime = nanotime
			}
		}
	}
	ids = make([]uint64, len(idset))
	i := 0
	for id := range idset {
		ids[i] = id
		i++
	}
	return
}

func (gormDB *GormDB) deleteCorrespondents(ctx context.Context, threshold int64) {
	result := gormDB.db.WithContext(ctx).Raw(`DELETE FROM correspondents WHERE nanotime <= (?);`, threshold)
	err := result.Error
	if err != nil {
		gormDB.logError("error deleting correspondents", err)
	} else {
		gormDB.logger.Debug(fmt.Sprintf("deleted %d correspondents entries", result.RowsAffected))
	}
}

// wait for forget queue items and process them one by one
func (gormDB *GormDB) forgetLoop() {
	defer func() {
		if r := recover(); r != nil {
			gormDB.logger.Error("gormDB",
				fmt.Sprintf("Panic in forget routine: %v\n%s", r, debug.Stack()))
			time.Sleep(cleanupPauseTime)
			go gormDB.forgetLoop()
		}
	}()

	for {
		for {
			found, err := gormDB.doForget()
			gormDB.logError("error processing forget", err)
			if err != nil {
				time.Sleep(cleanupPauseTime)
			}
			if !found {
				break
			}
		}

		<-gormDB.wakeForgetter
	}
}

// dequeue an item from the forget queue and process it
func (gormDB *GormDB) doForget() (found bool, err error) {
	id, account, err := func() (id int64, account string, err error) {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupPauseTime)
		defer cancel()

		row := gormDB.db.WithContext(ctx).Raw(
			`SELECT forget.id, forget.account FROM forget LIMIT 1;`).Row()
		err = row.Scan(&id, &account)
		if err == sql.ErrNoRows {
			return 0, "", nil
		}
		return
	}()

	if err != nil || account == "" {
		return false, err
	}

	found = true

	var count int
	for {
		start := time.Now()
		count, err = gormDB.doForgetIteration(account)
		elapsed := time.Since(start)
		if err != nil {
			return true, err
		}
		if count == 0 {
			break
		}
		time.Sleep(elapsed)
	}

	gormDB.logger.Debug("gormDB", "forget complete for account", account)

	ctx, cancel := context.WithTimeout(context.Background(), cleanupPauseTime)
	defer cancel()
	err = gormDB.db.WithContext(ctx).Exec(`DELETE FROM forget where id = ?;`, id).Error
	return
}

func (gormDB *GormDB) doForgetIteration(account string) (count int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupPauseTime)
	defer cancel()

	rows, err := gormDB.db.WithContext(ctx).Raw(`
		SELECT account_messages.history_id
		FROM account_messages
		WHERE account_messages.account = ?
		LIMIT ?;`, account, cleanupRowLimit).Rows()
	if err != nil {
		return
	}
	defer rows.Close()

	var ids []uint64
	for rows.Next() {
		var id uint64
		err = rows.Scan(&id)
		if err != nil {
			return
		}
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return
	}

	gormDB.logger.Debug("gormDB", fmt.Sprintf("deleting %d history rows from account %s", len(ids), account))
	err = gormDB.deleteHistoryIDs(ctx, ids)
	return len(ids), err
}

func (gormDB *GormDB) prepareStatements() (err error) {
	gormDB.insertHistory = (`INSERT INTO history
		(data, msgid) VALUES (?, ?);`)
	gormDB.insertSequence = (`INSERT INTO sequence
		(target, nanotime, history_id) VALUES (?, ?, ?);`)
	gormDB.insertConversation = (`INSERT INTO conversations
		(target, correspondent, nanotime, history_id) VALUES (?, ?, ?, ?);`)
	switch gormDB.config.Driver {
	case "mysql":
		gormDB.insertCorrespondent = (`INSERT INTO correspondents
			(target, correspondent, nanotime) VALUES (?, ?, ?)
			ON DUPLICATE KEY UPDATE nanotime = GREATEST(nanotime, ?);`)
	case "postgres":
		gormDB.insertCorrespondent = (`INSERT INTO correspondents
			(target, correspondent, nanotime) VALUES (?, ?, ?)
			ON CONFLICT (target, correspondent)
			DO UPDATE SET nanotime = GREATEST(EXCLUDED.nanotime, $4);`)
	}
	gormDB.insertAccountMessage = (`INSERT INTO account_messages
		(history_id, account) VALUES (?, ?);`)

	return
}

func (gormDB *GormDB) getTimeout() time.Duration {
	return time.Duration(gormDB.timeout.Load())
}

func (gormDB *GormDB) isTrackingAccountMessages() bool {
	return gormDB.trackAccountMessages.Load() != 0
}

func (gormDB *GormDB) logError(context string, err error) (quit bool) {
	if err != nil {
		gormDB.logger.Error("gormDB", context, err.Error())
		return true
	}
	return false
}

func (gormDB *GormDB) Forget(account string) {
	if gormDB.db == nil || account == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), gormDB.getTimeout())
	defer cancel()

	err := gormDB.db.WithContext(ctx).Exec(`INSERT INTO forget (account) VALUES (?);`, account).Error
	if gormDB.logError("can't insert into forget table", err) {
		return
	}

	// wake up the forget goroutine if it's blocked:
	select {
	case gormDB.wakeForgetter <- e{}:
	default:
	}
}

func (gormDB *GormDB) AddChannelItem(target string, item history.Item, account string) (err error) {
	if gormDB.db == nil {
		return
	}

	if target == "" {
		return utils.ErrInvalidParams
	}

	ctx, cancel := context.WithTimeout(context.Background(), gormDB.getTimeout())
	defer cancel()

	id, err := gormDB.insertBase(ctx, item)
	if err != nil {
		return
	}

	err = gormDB.insertSequenceEntry(ctx, target, item.Message.Time.UnixNano(), id)
	if err != nil {
		return
	}

	err = gormDB.insertAccountMessageEntry(ctx, id, account)
	if err != nil {
		return
	}

	return
}

func (gormDB *GormDB) insertSequenceEntry(ctx context.Context, target string, messageTime int64, id int64) (err error) {
	err = gormDB.db.Session(&gorm.Session{PrepareStmt: true}).WithContext(ctx).Exec(gormDB.insertSequence, target, messageTime, id).Error
	gormDB.logError("could not insert sequence entry", err)
	return
}

func (gormDB *GormDB) insertConversationEntry(ctx context.Context, target, correspondent string, messageTime int64, id int64) (err error) {
	err = gormDB.db.Session(&gorm.Session{PrepareStmt: true}).WithContext(ctx).Exec(gormDB.insertConversation, target, correspondent, messageTime, id).Error
	gormDB.logError("could not insert conversations entry", err)
	return
}

func (gormDB *GormDB) insertCorrespondentsEntry(ctx context.Context, target, correspondent string, messageTime int64, historyId int64) (err error) {
	err = gormDB.db.Session(&gorm.Session{PrepareStmt: true}).WithContext(ctx).Exec(gormDB.insertCorrespondent, target, correspondent, messageTime, messageTime).Error
	gormDB.logError("could not insert conversations entry", err)
	return
}

func (gormDB *GormDB) insertBase(ctx context.Context, item history.Item) (id int64, err error) {
	value, err := marshalItem(&item)
	if gormDB.logError("could not marshal item", err) {
		return
	}

	result := model.History{
		MsgID: item.Message.Msgid,
		Data:  datatypes.JSON([]byte(value)),
	}
	err = gormDB.db.Session(&gorm.Session{PrepareStmt: true}).WithContext(ctx).Model(&model.History{}).Create(&result).Error
	if gormDB.logError("could not insert item", err) {
		return
	}
	if result.ID == 0 {
		err = fmt.Errorf("could not insert item")
	}
	if gormDB.logError("could not insert item", err) {
		return
	}

	id = result.ID

	return
}

func (gormDB *GormDB) insertAccountMessageEntry(ctx context.Context, id int64, account string) (err error) {
	if account == "" || !gormDB.isTrackingAccountMessages() {
		return
	}
	err = gormDB.db.Session(&gorm.Session{PrepareStmt: true}).WithContext(ctx).Exec(gormDB.insertAccountMessage, id, account).Error
	gormDB.logError("could not insert account-message entry", err)
	return
}

func (gormDB *GormDB) AddDirectMessage(sender, senderAccount, recipient, recipientAccount string, item history.Item) (err error) {
	if gormDB.db == nil {
		return
	}

	if senderAccount == "" && recipientAccount == "" {
		return
	}

	if sender == "" || recipient == "" {
		return utils.ErrInvalidParams
	}

	ctx, cancel := context.WithTimeout(context.Background(), gormDB.getTimeout())
	defer cancel()

	id, err := gormDB.insertBase(ctx, item)
	if err != nil {
		return
	}

	nanotime := item.Message.Time.UnixNano()

	if senderAccount != "" {
		err = gormDB.insertConversationEntry(ctx, senderAccount, recipient, nanotime, id)
		if err != nil {
			return
		}
		err = gormDB.insertCorrespondentsEntry(ctx, senderAccount, recipient, nanotime, id)
		if err != nil {
			return
		}
	}

	if recipientAccount != "" && sender != recipient {
		err = gormDB.insertConversationEntry(ctx, recipientAccount, sender, nanotime, id)
		if err != nil {
			return
		}
		err = gormDB.insertCorrespondentsEntry(ctx, recipientAccount, sender, nanotime, id)
		if err != nil {
			return
		}
	}

	err = gormDB.insertAccountMessageEntry(ctx, id, senderAccount)
	if err != nil {
		return
	}

	return
}

// note that accountName is the unfolded name
func (gormDB *GormDB) DeleteMsgid(msgid, accountName string) (err error) {
	if gormDB.db == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), gormDB.getTimeout())
	defer cancel()

	_, id, data, err := gormDB.lookupMsgid(ctx, msgid, true)
	if err != nil {
		return
	}

	if accountName != "*" {
		var item history.Item
		err = unmarshalItem(data, &item)
		// delete if the entry is corrupt
		if err == nil && item.AccountName != accountName {
			return ErrDisallowed
		}
	}

	err = gormDB.deleteHistoryIDs(ctx, []uint64{id})
	gormDB.logError("couldn't delete msgid", err)
	return
}

func (gormDB *GormDB) Export(account string, writer io.Writer) {
	if gormDB.db == nil {
		return
	}

	var err error
	var lastSeen uint64
	for {
		rows := func() (count int) {
			ctx, cancel := context.WithTimeout(context.Background(), cleanupPauseTime)
			defer cancel()

			rows, rowsErr := gormDB.db.WithContext(ctx).Raw(`
				SELECT account_messages.history_id, history.data, sequence.target FROM account_messages
				INNER JOIN history ON history.id = account_messages.history_id
				INNER JOIN sequence ON account_messages.history_id = sequence.history_id
				WHERE account_messages.account = ? AND account_messages.history_id > ?
				LIMIT ?`, account, lastSeen, cleanupRowLimit).Rows()
			if rowsErr != nil {
				err = rowsErr
				return
			}
			defer rows.Close()
			for rows.Next() {
				var id uint64
				var blob, jsonBlob []byte
				var target string
				var item history.Item
				err = rows.Scan(&id, &blob, &target)
				if err != nil {
					return
				}
				err = unmarshalItem(blob, &item)
				if err != nil {
					return
				}
				item.CfCorrespondent = target
				jsonBlob, err = json.Marshal(item)
				if err != nil {
					return
				}
				count++
				if lastSeen < id {
					lastSeen = id
				}
				writer.Write(jsonBlob)
				writer.Write([]byte{'\n'})
			}
			return
		}()
		if rows == 0 || err != nil {
			break
		}
	}

	gormDB.logError("could not export history", err)
	return
}

func (gormDB *GormDB) lookupMsgid(ctx context.Context, msgid string, includeData bool) (result time.Time, id uint64, data []byte, err error) {
	cols := `sequence.nanotime, conversations.nanotime`
	if includeData {
		cols = `sequence.nanotime, conversations.nanotime, history.id, history.data`
	}
	row := gormDB.db.WithContext(ctx).Raw(fmt.Sprintf(`
		SELECT %s FROM history
		LEFT JOIN sequence ON history.id = sequence.history_id
		LEFT JOIN conversations ON history.id = conversations.history_id
		WHERE history.msgid = ? LIMIT 1;`, cols), msgid).Row()
	var nanoSeq, nanoConv sql.NullInt64
	if !includeData {
		err = row.Scan(&nanoSeq, &nanoConv)
	} else {
		err = row.Scan(&nanoSeq, &nanoConv, &id, &data)
	}
	if err != sql.ErrNoRows {
		gormDB.logError("could not resolve msgid to time", err)
	}
	if err != nil {
		return
	}
	nanotime := extractNanotime(nanoSeq, nanoConv)
	if nanotime == 0 {
		err = sql.ErrNoRows
		return
	}
	result = time.Unix(0, nanotime).UTC()
	return
}

func extractNanotime(seq, conv sql.NullInt64) (result int64) {
	if seq.Valid {
		return seq.Int64
	} else if conv.Valid {
		return conv.Int64
	}
	return
}

func (gormDB *GormDB) selectItems(ctx context.Context, query string, args ...interface{}) (results []history.Item, err error) {
	rows, err := gormDB.db.WithContext(ctx).Raw(query, args...).Rows()
	if gormDB.logError("could not select history items", err) {
		return
	}

	defer rows.Close()

	for rows.Next() {
		var blob []byte
		var item history.Item
		err = rows.Scan(&blob)
		if gormDB.logError("could not scan history item", err) {
			return
		}
		err = unmarshalItem(blob, &item)
		if gormDB.logError("could not unmarshal history item", err) {
			return
		}
		results = append(results, item)
	}
	return
}

func (gormDB *GormDB) betweenTimestamps(ctx context.Context, target, correspondent string, after, before, cutoff time.Time, limit int) (results []history.Item, err error) {
	useSequence := correspondent == ""
	table := "sequence"
	if !useSequence {
		table = "conversations"
	}

	after, before, ascending := history.MinMaxAsc(after, before, cutoff)
	direction := "ASC"
	if !ascending {
		direction = "DESC"
	}

	var queryBuf strings.Builder

	args := make([]interface{}, 0, 6)
	fmt.Fprintf(&queryBuf,
		"SELECT history.data from history INNER JOIN %[1]s ON history.id = %[1]s.history_id WHERE", table)
	if useSequence {
		fmt.Fprintf(&queryBuf, " sequence.target = ?")
		args = append(args, target)
	} else {
		fmt.Fprintf(&queryBuf, " conversations.target = ? AND conversations.correspondent = ?")
		args = append(args, target)
		args = append(args, correspondent)
	}
	if !after.IsZero() {
		fmt.Fprintf(&queryBuf, " AND %s.nanotime > ?", table)
		args = append(args, after.UnixNano())
	}
	if !before.IsZero() {
		fmt.Fprintf(&queryBuf, " AND %s.nanotime < ?", table)
		args = append(args, before.UnixNano())
	}
	fmt.Fprintf(&queryBuf, " ORDER BY %[1]s.nanotime %[2]s LIMIT ?;", table, direction)
	args = append(args, limit)

	results, err = gormDB.selectItems(ctx, queryBuf.String(), args...)
	if err == nil && !ascending {
		slices.Reverse(results)
	}
	return
}

func (gormDB *GormDB) listCorrespondentsInternal(ctx context.Context, target string, after, before, cutoff time.Time, limit int) (results []history.TargetListing, err error) {
	after, before, ascending := history.MinMaxAsc(after, before, cutoff)
	direction := "ASC"
	if !ascending {
		direction = "DESC"
	}

	var queryBuf strings.Builder
	args := make([]interface{}, 0, 4)
	queryBuf.WriteString(`SELECT correspondents.correspondent, correspondents.nanotime from correspondents
		WHERE target = ?`)
	args = append(args, target)
	if !after.IsZero() {
		queryBuf.WriteString(" AND correspondents.nanotime > ?")
		args = append(args, after.UnixNano())
	}
	if !before.IsZero() {
		queryBuf.WriteString(" AND correspondents.nanotime < ?")
		args = append(args, before.UnixNano())
	}
	fmt.Fprintf(&queryBuf, " ORDER BY correspondents.nanotime %s LIMIT ?;", direction)
	args = append(args, limit)
	query := queryBuf.String()

	rows, err := gormDB.db.WithContext(ctx).Raw(query, args...).Rows()
	if err != nil {
		return
	}
	defer rows.Close()
	var correspondent string
	var nanotime int64
	for rows.Next() {
		err = rows.Scan(&correspondent, &nanotime)
		if err != nil {
			return
		}
		results = append(results, history.TargetListing{
			CfName: correspondent,
			Time:   time.Unix(0, nanotime),
		})
	}

	if !ascending {
		slices.Reverse(results)
	}

	return
}

func (gormDB *GormDB) ListChannels(cfchannels []string) (results []history.TargetListing, err error) {
	if gormDB.db == nil {
		return
	}

	if len(cfchannels) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), gormDB.getTimeout())
	defer cancel()

	var queryBuf strings.Builder
	args := make([]interface{}, 0, len(results))
	// https://dev.gormDB.com/doc/refman/8.0/en/group-by-optimization.html
	// this should be a "loose index scan"
	queryBuf.WriteString(`SELECT sequence.target, MAX(sequence.nanotime) FROM sequence
		WHERE sequence.target IN (`)
	for i, chname := range cfchannels {
		if i != 0 {
			queryBuf.WriteString(", ")
		}
		queryBuf.WriteByte('?')
		args = append(args, chname)
	}
	queryBuf.WriteString(") GROUP BY sequence.target;")

	rows, err := gormDB.db.WithContext(ctx).Raw(queryBuf.String(), args...).Rows()
	if gormDB.logError("could not query channel listings", err) {
		return
	}
	defer rows.Close()

	var target string
	var nanotime int64
	for rows.Next() {
		err = rows.Scan(&target, &nanotime)
		if gormDB.logError("could not scan channel listings", err) {
			return
		}
		results = append(results, history.TargetListing{
			CfName: target,
			Time:   time.Unix(0, nanotime),
		})
	}
	return
}

func (gormDB *GormDB) Close() {
	// closing the database will close our prepared statements as well
	if gormDB.db != nil {
		sqlDB, err := gormDB.db.DB()
		if err != nil {
			gormDB.logger.Error("gormDB", "could not get underlying database handle", err.Error())
		} else {
			sqlDB.Close()
		}
	}
	gormDB.db = nil
}

// implements history.Sequence, emulating a single history buffer (for a channel,
// a single user's DMs, or a DM conversation)
type mySQLHistorySequence struct {
	gormDB        *GormDB
	target        string
	correspondent string
	cutoff        time.Time
}

func (s *mySQLHistorySequence) Between(start, end history.Selector, limit int) (results []history.Item, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.gormDB.getTimeout())
	defer cancel()

	startTime := start.Time
	if start.Msgid != "" {
		startTime, _, _, err = s.gormDB.lookupMsgid(ctx, start.Msgid, false)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			} else {
				return nil, err
			}
		}
	}
	endTime := end.Time
	if end.Msgid != "" {
		endTime, _, _, err = s.gormDB.lookupMsgid(ctx, end.Msgid, false)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			} else {
				return nil, err
			}
		}
	}

	results, err = s.gormDB.betweenTimestamps(ctx, s.target, s.correspondent, startTime, endTime, s.cutoff, limit)
	return results, err
}

func (s *mySQLHistorySequence) Around(start history.Selector, limit int) (results []history.Item, err error) {
	return history.GenericAround(s, start, limit)
}

func (seq *mySQLHistorySequence) ListCorrespondents(start, end history.Selector, limit int) (results []history.TargetListing, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), seq.gormDB.getTimeout())
	defer cancel()

	// TODO accept msgids here?
	startTime := start.Time
	endTime := end.Time

	results, err = seq.gormDB.listCorrespondentsInternal(ctx, seq.target, startTime, endTime, seq.cutoff, limit)
	seq.gormDB.logError("could not read correspondents", err)
	return
}

func (seq *mySQLHistorySequence) Cutoff() time.Time {
	return seq.cutoff
}

func (seq *mySQLHistorySequence) Ephemeral() bool {
	return false
}

func (gormDB *GormDB) MakeSequence(target, correspondent string, cutoff time.Time) history.Sequence {
	return &mySQLHistorySequence{
		target:        target,
		correspondent: correspondent,
		gormDB:        gormDB,
		cutoff:        cutoff,
	}
}
