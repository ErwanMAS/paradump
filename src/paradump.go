// ------------------------------------------------------------------------------------------
package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"os"
	"reflect"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/klauspost/compress/zstd"
)

/* ------------------------------------------------------------------------------------------
   ParaDump is a tool that will create a dump of mysql in table , by using multiple threads
   to read tables .


   https://mariadb.com/kb/en/enhancements-for-start-transaction-with-consistent-snapshot/
   https://docs.percona.com/percona-server/5.7/management/start_transaction_with_consistent_snapshot.html#


   https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_protocol_compression_algorithms

   ------------------------------------------------------------------------------------------

   env GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" -v paradump.go

   go build   -ldflags "-s -w" -v paradump.go

   ./paradump  -host 127.0.0.1 -schema foobar -port 4000 -user foobar -pwd test1234 -table client_activity -table client_info -dumpfile /dev/null

   ------------------------------------------------------------------------------------------ */

/* ------------------------------------------------------------------------------------------
   https://go.dev/blog/strings

   https://go.dev/blog/slices

   https://go.dev/doc/asm

   https://cmc.gitbook.io/go-internals/

   https://chris124567.github.io/2021-06-21-go-performance/

   http://go-database-sql.org/importing.html

   https://gobyexample.com/

   https://pkg.go.dev/fmt#Sprintf

   https://pkg.go.dev/database/sql

   https://go.dev/tour/moretypes/15    ( slice )

   https://pkg.go.dev/flag@go1.19.4

   https://www.antoniojgutierrez.com/posts/2021-05-14-short-and-long-options-in-go-flags-pkg/

   https://pkg.go.dev/reflect#DeepEqual

   https://github.com/JamesStewy/go-mysqldump

   https://stackoverflow.com/questions/34861479/how-to-detect-when-bytes-cant-be-converted-to-string-in-go
   ------------------------------------------------------------------------------------------ */
// ------------------------------------------------------------------------------------------
var (
	mode_debug bool
	mode_trace bool
)

// ------------------------------------------------------------------------------------------
type InfoMysqlPosition struct {
	Name string
	Pos  int
}

// ------------------------------------------------------------------------------------------
func MysqlLockTableWaitRelease(jobsync chan bool, conn *sql.Conn, myPos chan InfoMysqlPosition) {
	var ret_val InfoMysqlPosition
	// --------------------
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := conn.PingContext(ctx)
	if p_err != nil {
		log.Fatal("can not ping")
	}
	ctx, _ = context.WithTimeout(context.Background(), 30*time.Second)
	_, e_err := conn.ExecContext(ctx, "FLUSH TABLES;")
	if e_err != nil {
		log.Print("can not flush tables")
		log.Fatal("%s", e_err.Error())
	}
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
	_, r_err := conn.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK;")
	if r_err != nil {
		log.Fatal("can not flush tables")
	}
	jobsync <- true
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
	allrows, q_err := conn.QueryContext(ctx, "show master status;")
	if q_err != nil {
		log.Print("can not get master position")
		log.Fatal("%s", q_err.Error())
	}
	for allrows.Next() {
		var v_bin_name string
		var v_bin_pos string
		var v_str_1 string
		var v_str_2 string
		var v_str_3 string
		err := allrows.Scan(&v_bin_name, &v_bin_pos, &v_str_1, &v_str_2, &v_str_3)
		if err != nil {
			log.Print("can not Scan master position")
			log.Fatal(err.Error())
		}
		ret_val.Name = v_bin_name
		ret_val.Pos, _ = strconv.Atoi(v_bin_pos)
	}
	<-jobsync
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	_, u_err := conn.ExecContext(ctx, "UNLOCK TABLES;")
	if u_err != nil {
		log.Fatalf("can not unlock tabless\n%s\n", u_err.Error())
	}
	jobsync <- true
	myPos <- ret_val
}

// ------------------------------------------------------------------------------------------
type InfoMysqlSession struct {
	Status   bool
	cnxId    int
	Position InfoMysqlPosition
}
type StatMysqlSession struct {
	Cnt      int
	FileName string
	FilePos  int
}

func MysqlLockTableStartConsistenRead(infoconn chan InfoMysqlSession, myId int, conn *sql.Conn, jobstart chan bool, jobsync chan int) {
	var ret_val InfoMysqlSession
	ret_val.Status = false
	ret_val.cnxId = myId
	// --------------------
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := conn.PingContext(ctx)
	if p_err != nil {
		log.Fatal("can not ping")
	}
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
	_, e_err := conn.ExecContext(ctx, "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
	if e_err != nil {
		log.Printf("thread %d , can not set NAMES for the session\n%s\n", myId, e_err.Error())
		infoconn <- ret_val
		return
	}
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
	_, t_err := conn.ExecContext(ctx, "SET TIME_ZONE='+00:00' ")
	if t_err != nil {
		log.Printf("thread %d , can not set TIME_ZONE for the session\n%s\n", myId, t_err.Error())
		infoconn <- ret_val
		return
	}
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
	_, l_err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL  REPEATABLE READ ")
	if l_err != nil {
		log.Printf("thread %d , can not set REPEATABLE READ for the session\n%s\n", myId, l_err.Error())
		infoconn <- ret_val
		return
	}
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
	_, w_err := conn.ExecContext(ctx, "SET SESSION wait_timeout=86400 ")
	if w_err != nil {
		log.Printf("thread %d , can not set wait_timeout\n%s\n", myId, w_err.Error())
		infoconn <- ret_val
		return
	}
	// -------------------------------------------------------------------------------
	// we signal we are ready
	jobsync <- 1
	if mode_debug {
		log.Printf("thread %d , we are ready\n", myId)
	}
	// -------------------------------------------------------------------------------
	// we wait for signal
	<-jobstart
	// -------------------------------------------------------------------------------
	if mode_debug {
		log.Printf("start stransaction for %d", myId)
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	_, s_err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT;")
	if s_err != nil {
		log.Fatalf("can not start transaction with consistent read\n%s\n", s_err.Error())
	}
	allrows, q_err := conn.QueryContext(ctx, "show master status;")
	if q_err != nil {
		log.Fatalf("can not get master status\n%s", q_err.Error())
	}
	jobsync <- 2
	// -------------------------------------------------------------------------------
	for allrows.Next() {
		var v_bin_name string
		var v_bin_pos string
		var v_str_1 string
		var v_str_2 string
		var v_str_3 string
		err := allrows.Scan(&v_bin_name, &v_bin_pos, &v_str_1, &v_str_2, &v_str_3)
		if err != nil {
			log.Print("can not Scan master position")
			log.Fatal(err.Error())
		}
		ret_val.Position.Name = v_bin_name
		ret_val.Position.Pos, _ = strconv.Atoi(v_bin_pos)
	}
	if mode_debug {
		log.Printf("done start transaction for %d we are at %s@%d ", myId, ret_val.Position.Name, ret_val.Position.Pos)
	}
	ret_val.Status = true
	infoconn <- ret_val
}

// ------------------------------------------------------------------------------------------
func GetaSynchronizedMysqlConnections(DbHost string, DbPort int, DbUsername string, DbUserPassword string, TargetCount int, ConDatabase string) (*sql.DB, []*sql.Conn, StatMysqlSession, error) {
	// db, err := sql.Open("pgx", "postgres://root@localhost:26257/defaultdb?sslmode=disable")
	// select pg_export_snapshot();
	// set transaction snapshot ‘00000003-000021CE-1’;
	// https://www.postgresql.org/docs/10/sql-set-transaction.html
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?maxAllowedPacket=0", DbUsername, DbUserPassword, DbHost, DbPort, ConDatabase))
	if err != nil {
		log.Print("can not create a mysql object")
		log.Fatal(err.Error())
	}
	db.SetMaxOpenConns(TargetCount * 3)
	db.SetMaxIdleConns(TargetCount * 3)
	// --------------------
	var ctx context.Context
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	// --------------------
	db_conns := make([]*sql.Conn, TargetCount*3)
	for i := 0; i < TargetCount*3; i++ {
		first_conn, err := db.Conn(ctx)
		if err != nil {
			log.Print("can not open a mysql connection")
			log.Fatal(err.Error())
		}
		db_conns[i] = first_conn
	}
	// --------------------------------------------------------------------------
	globallockchan := make(chan bool)
	globalposchan := make(chan InfoMysqlPosition)
	resultreadchan := make(chan InfoMysqlSession, TargetCount*3-1)
	startreadchan := make(chan bool, TargetCount*3-1)
	syncreadchan := make(chan int, TargetCount*3-1)
	// -------------------------------------------
	for i := 1; i < TargetCount*3; i++ {
		go MysqlLockTableStartConsistenRead(resultreadchan, i, db_conns[i], startreadchan, syncreadchan)
	}
	// --------------------
	// we wait for TargetCount*3-1  feedback - ready to start a transaction
	for i := 1; i < TargetCount*3; i++ {
		<-syncreadchan
	}
	log.Print("everyone is ready")
	// --------------------
	// we start the read lock
	go MysqlLockTableWaitRelease(globallockchan, db_conns[0], globalposchan)
	<-globallockchan
	log.Print("ok for global lock")
	// --------------------
	// we wake all read
	for i := 1; i < TargetCount*3; i++ {
		startreadchan <- true
	}
	// --------------------
	// we wait for TargetCount*3-1  feedback - a read trasnsaction is started
	for i := 1; i < TargetCount*3; i++ {
		<-syncreadchan
	}
	// --------------------
	// we release the global lock
	globallockchan <- true
	log.Print("ok as for release the global lock")
	// --------------------------------------------------------------------------
	var stats_ses []StatMysqlSession
	db_sessions_filepos := make([]InfoMysqlSession, TargetCount*3-1)
	<-globallockchan
	masterpos := <-globalposchan
	for i := 1; i < TargetCount*3; i++ {
		db_sessions_filepos[i-1] = <-resultreadchan
		foundidx := -1
		for j := 0; j < len(stats_ses); j++ {
			if foundidx == -1 && stats_ses[j].FileName == db_sessions_filepos[i-1].Position.Name && stats_ses[j].FilePos == db_sessions_filepos[i-1].Position.Pos {
				stats_ses[j].Cnt++
				foundidx = j
			}
		}
		if foundidx == -1 {
			stats_ses = append(stats_ses, StatMysqlSession{Cnt: 1, FileName: db_sessions_filepos[i-1].Position.Name, FilePos: db_sessions_filepos[i-1].Position.Pos})
		}
	}
	// --------------------
	log.Printf("we collected infos about %d sessions differents postions count is %d", len(db_sessions_filepos), len(stats_ses))
	foundRefPos := -1
	for j := 0; foundRefPos == -1 && j < len(stats_ses); j++ {
		if stats_ses[j].Cnt >= TargetCount {
			foundRefPos = j
		}
	}
	// --------------------
	log.Printf("we choose session with pos %s@%d", stats_ses[foundRefPos].FileName, stats_ses[foundRefPos].FilePos)
	if mode_debug {
		log.Printf("master position was %s@%d", masterpos.Name, masterpos.Pos)
	}
	// --------------------
	if masterpos.Name != stats_ses[foundRefPos].FileName || stats_ses[foundRefPos].FilePos != masterpos.Pos {
		log.Fatal(" we choose a session that have a different position than the first session ")
	}
	// --------------------
	var ret_dbconns []*sql.Conn
	if foundRefPos >= 0 {
		for i := 0; i < TargetCount*3-1; i++ {
			if db_sessions_filepos[i].Position.Name == stats_ses[foundRefPos].FileName && db_sessions_filepos[i].Position.Pos == stats_ses[foundRefPos].FilePos && len(ret_dbconns) < TargetCount {
				ret_dbconns = append(ret_dbconns, db_conns[db_sessions_filepos[i].cnxId])
				db_conns[db_sessions_filepos[i].cnxId] = nil
			}
		}
	}
	for i := 0; i < TargetCount*3; i++ {
		if db_conns[i] != nil {
			db_conns[i].Close()
		}
	}
	// --------------------
	return db, ret_dbconns, stats_ses[foundRefPos], nil
}

// ------------------------------------------------------------------------------------------
func GetDstMysqlConnections(DbHost string, DbPort int, DbUsername string, DbUserPassword string, TargetCount int, ConDatabase string) (*sql.DB, []*sql.Conn, error) {
	// db, err := sql.Open("pgx", "postgres://root@localhost:26257/defaultdb?sslmode=disable")
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?maxAllowedPacket=0", DbUsername, DbUserPassword, DbHost, DbPort, ConDatabase))
	if err != nil {
		log.Print("can not create a mysql object")
		log.Fatal(err.Error())
	}
	db.SetMaxOpenConns(TargetCount)
	db.SetMaxIdleConns(TargetCount)
	// --------------------
	var ctx context.Context
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	// --------------------
	db_conns := make([]*sql.Conn, TargetCount)
	for i := 0; i < TargetCount; i++ {
		first_conn, err := db.Conn(ctx)
		if err != nil {
			log.Print("can not open a mysql connection")
			log.Fatal(err.Error())
		}
		db_conns[i] = first_conn
		ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
		_, e_err := db_conns[i].ExecContext(ctx, "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
		if e_err != nil {
			log.Fatalf("thread %d , can not set NAMES for the session\n%s\n", i, e_err.Error())
		}
		ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
		_, t_err := db_conns[i].ExecContext(ctx, "SET TIME_ZONE='+00:00' ")
		if t_err != nil {
			log.Fatalf("thread %d , can not set TIME_ZONE for the session\n%s\n", i, t_err.Error())
		}

	}
	// --------------------
	return db, db_conns, nil
}

// ------------------------------------------------------------------------------------------
func GetaSynchronizedPostgresConnections(DbHost string, DbPort int, DbUsername string, DbUserPassword string, TargetCount int, ConDatabase string) (*sql.DB, []*sql.Conn, StatMysqlSession, error) {
	// db, err := sql.Open("pgx", "postgres://root@localhost:26257/defaultdb?sslmode=disable")
	// select pg_export_snapshot();
	// set transaction snapshot ‘00000003-000021CE-1’;
	// https://www.postgresql.org/docs/10/sql-set-transaction.html
	db, err := sql.Open("pgx", fmt.Sprintf("postgres://%s@%s:%s:%d/%s?sslmode=disable", DbUsername, DbUserPassword, DbHost, DbPort, ConDatabase))
	if err != nil {
		log.Print("can not create a postgres object")
		log.Fatal(err.Error())
	}
	db.SetMaxOpenConns(TargetCount * 3)
	db.SetMaxIdleConns(TargetCount * 3)
	// --------------------
	var ctx context.Context
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	// --------------------
	db_conns := make([]*sql.Conn, TargetCount*3)
	for i := 0; i < TargetCount*3; i++ {
		first_conn, err := db.Conn(ctx)
		if err != nil {
			log.Print("can not open a mysql connection")
			log.Fatal(err.Error())
		}
		db_conns[i] = first_conn
	}
	// --------------------------------------------------------------------------
	globallockchan := make(chan bool)
	globalposchan := make(chan InfoMysqlPosition)
	resultreadchan := make(chan InfoMysqlSession, TargetCount*3-1)
	startreadchan := make(chan bool, TargetCount*3-1)
	syncreadchan := make(chan int, TargetCount*3-1)
	// -------------------------------------------
	for i := 1; i < TargetCount*3; i++ {
		go MysqlLockTableStartConsistenRead(resultreadchan, i, db_conns[i], startreadchan, syncreadchan)
	}
	// --------------------
	// we wait for TargetCount*3-1  feedback - ready to start a transaction
	for i := 1; i < TargetCount*3; i++ {
		<-syncreadchan
	}
	log.Print("everyone is ready")
	// --------------------
	// we start the read lock
	go MysqlLockTableWaitRelease(globallockchan, db_conns[0], globalposchan)
	<-globallockchan
	log.Print("ok for global lock")
	// --------------------
	// we wake all read
	for i := 1; i < TargetCount*3; i++ {
		startreadchan <- true
	}
	// --------------------
	// we wait for TargetCount*3-1  feedback - a read trasnsaction is started
	for i := 1; i < TargetCount*3; i++ {
		<-syncreadchan
	}
	// --------------------
	// we release the global lock
	globallockchan <- true
	log.Print("ok as for release the global lock")
	// --------------------------------------------------------------------------
	var stats_ses []StatMysqlSession
	db_sessions_filepos := make([]InfoMysqlSession, TargetCount*3-1)
	<-globallockchan
	masterpos := <-globalposchan
	for i := 1; i < TargetCount*3; i++ {
		db_sessions_filepos[i-1] = <-resultreadchan
		foundidx := -1
		for j := 0; j < len(stats_ses); j++ {
			if foundidx == -1 && stats_ses[j].FileName == db_sessions_filepos[i-1].Position.Name && stats_ses[j].FilePos == db_sessions_filepos[i-1].Position.Pos {
				stats_ses[j].Cnt++
				foundidx = j
			}
		}
		if foundidx == -1 {
			stats_ses = append(stats_ses, StatMysqlSession{Cnt: 1, FileName: db_sessions_filepos[i-1].Position.Name, FilePos: db_sessions_filepos[i-1].Position.Pos})
		}
	}
	// --------------------
	log.Printf("we collected infos about %d sessions differents postions count is %d", len(db_sessions_filepos), len(stats_ses))
	foundRefPos := -1
	for j := 0; foundRefPos == -1 && j < len(stats_ses); j++ {
		if stats_ses[j].Cnt >= TargetCount {
			foundRefPos = j
		}
	}
	// --------------------
	log.Printf("we choose session with pos %s@%d", stats_ses[foundRefPos].FileName, stats_ses[foundRefPos].FilePos)
	if mode_debug {
		log.Printf("master position was %s@%d", masterpos.Name, masterpos.Pos)
	}
	// --------------------
	if masterpos.Name != stats_ses[foundRefPos].FileName || stats_ses[foundRefPos].FilePos != masterpos.Pos {
		log.Fatal(" we choose a session that have a different position than the first session ")
	}
	// --------------------
	var ret_dbconns []*sql.Conn
	if foundRefPos >= 0 {
		for i := 0; i < TargetCount*3-1; i++ {
			if db_sessions_filepos[i].Position.Name == stats_ses[foundRefPos].FileName && db_sessions_filepos[i].Position.Pos == stats_ses[foundRefPos].FilePos && len(ret_dbconns) < TargetCount {
				ret_dbconns = append(ret_dbconns, db_conns[db_sessions_filepos[i].cnxId])
				db_conns[db_sessions_filepos[i].cnxId] = nil
			}
		}
	}
	for i := 0; i < TargetCount*3; i++ {
		if db_conns[i] != nil {
			db_conns[i].Close()
		}
	}
	// --------------------
	return db, ret_dbconns, stats_ses[foundRefPos], nil
}

// ------------------------------------------------------------------------------------------
func GetDstPostgresConnections(DbHost string, DbPort int, DbUsername string, DbUserPassword string, TargetCount int, ConDatabase string) (*sql.DB, []*sql.Conn, error) {
	// db, err := sql.Open("pgx", "postgres://root@localhost:26257/defaultdb?sslmode=disable")
	db, err := sql.Open("pgx", fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", DbUsername, DbUserPassword, DbHost, DbPort, ConDatabase))
	if err != nil {
		log.Print("can not create a postgres object")
		log.Fatal(err.Error())
	}
	db.SetMaxOpenConns(TargetCount)
	db.SetMaxIdleConns(TargetCount)
	// --------------------
	var ctx context.Context
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	// --------------------
	db_conns := make([]*sql.Conn, TargetCount)
	for i := 0; i < TargetCount; i++ {
		first_conn, err := db.Conn(ctx)
		if err != nil {
			log.Print("can not open a postgres connection")
			log.Fatal(err.Error())
		}
		db_conns[i] = first_conn
		ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
		_, e_err := db_conns[i].ExecContext(ctx, "SET client_encoding = 'UTF8';")
		if e_err != nil {
			log.Fatalf("thread %d , can not set client_encoding for the session\n%s\n", i, e_err.Error())
		}
		ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
		_, t_err := db_conns[i].ExecContext(ctx, "SET time zone 'UTC'; ")
		if t_err != nil {
			log.Fatalf("thread %d , can not set time zone for the session\n%s\n", i, t_err.Error())
		}
		ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
		_, r_err := db_conns[i].ExecContext(ctx, "SET session_replication_role = replica; ")
		if r_err != nil {
			log.Fatalf("thread %d , can not set session_replication_role for the session\n%s\n", i, r_err.Error())
		}
	}
	// --------------------
	return db, db_conns, nil
}

// ------------------------------------------------------------------------------------------
type columnInfo struct {
	colName      string
	colType      string
	colSqlType   string
	dtPrec       int
	nuPrec       int
	isNullable   bool
	mustBeQuote  bool
	haveFract    bool
	isKindChar   bool
	isKindBinary bool
	isKindFloat  bool
}

type indexInfo struct {
	idxName     string
	cardinality int64
	columns     []columnInfo
}

type aTable struct {
	dbName string
	tbName string
}

type MetadataTable struct {
	dbName                         string
	tbName                         string
	isEmpty                        bool
	cntRows                        int64
	sizeBytes                      int64
	storageEng                     string
	cntCols                        int
	cntPkCols                      int
	columnInfos                    []columnInfo
	primaryKey                     []string
	Indexes                        []indexInfo
	fakePrimaryKey                 bool
	withTrigger                    bool
	onError                        int
	fullName                       string
	listColsSQL                    string
	listColsPkSQL                  string
	listColsPkFetchSQL             string
	listColsPkOrderSQL             string
	listColsPkOrderDescSQL         string
	listColsCSV                    string
	query_for_browser_first        string
	query_for_browser_next         string
	param_indices_browser_next_qry []int
	query_for_reader_interval      string
	param_indices_interval_lo_qry  []int
	param_indices_interval_up_qry  []int
	query_for_reader_equality      string
	param_indices_equality_qry     []int
	query_for_insert               string
}

// ------------------------------------------------------------------------------------------
func GetMysqlBasicMetadataInfo(adbConn *sql.Conn, dbName string, tableName string) (MetadataTable, bool) {
	var result MetadataTable
	result.dbName = dbName
	result.tbName = tableName
	result.fakePrimaryKey = false
	result.onError = 0
	result.isEmpty = true
	result.withTrigger = false

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := adbConn.PingContext(ctx)
	if p_err != nil {
		log.Fatalf("can not ping\n%s", p_err.Error())
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err := adbConn.QueryContext(ctx, "select coalesce(data_length + index_length,-1),coalesce(TABLE_ROWS,-1),coalesce(ENGINE,'UNKNOW'),TABLE_TYPE from information_schema.tables WHERE table_schema = ? AND table_name = ?     ", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query information_schema.tables for %s.%s\n%s", dbName, tableName, q_err.Error())
	}
	typeTable := "DO_NOT_EXIST"
	for q_rows.Next() {
		err := q_rows.Scan(&result.sizeBytes, &result.cntRows, &result.storageEng, &typeTable)
		if err != nil {
			log.Printf("can not query information_schema.tables for %s.%s", dbName, tableName)
			log.Fatal(err.Error())
		}
		if result.storageEng != "InnoDB" {
			result.onError = result.onError | 4
		}
		if typeTable != "BASE TABLE" {
			result.onError = result.onError | 8
		}
	}
	if typeTable == "DO_NOT_EXIST" {
		result.onError = result.onError | 16
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err = adbConn.QueryContext(ctx, "select COLUMN_NAME , DATA_TYPE,IS_NULLABLE,IFNULL(DATETIME_PRECISION,-9999),IFNULL(NUMERIC_PRECISION,-9999),COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? order by ORDINAL_POSITION ", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query information_schema.columns for %s.%s\n%s", dbName, tableName, q_err.Error())
	}
	for q_rows.Next() {
		var a_col columnInfo
		var a_str string
		err := q_rows.Scan(&a_col.colName, &a_col.colType, &a_str, &a_col.dtPrec, &a_col.nuPrec, &a_col.colSqlType)
		if err != nil {
			log.Print("can not scan columns informations")
			log.Fatal(err.Error())
		}
		a_col.isNullable = (a_str == "YES")
		a_col.isKindChar = (a_col.colType == "char" || a_col.colType == "longtext" || a_col.colType == "mediumtext" || a_col.colType == "text" || a_col.colType == "tinytext" || a_col.colType == "varchar" || a_col.colType == "enum")
		a_col.isKindBinary = (a_col.colType == "varbinary" || a_col.colType == "binary" || a_col.colType == "tinyblob" || a_col.colType == "blob" || a_col.colType == "longblob" || a_col.colType == "bit")
		a_col.mustBeQuote = a_col.isKindChar || a_col.isKindBinary || (a_col.colType == "date" || a_col.colType == "datetime" || a_col.colType == "time" || a_col.colType == "timestamp")
		a_col.isKindFloat = (a_col.colType == "float" || a_col.colType == "double")
		a_col.haveFract = (a_col.colType == "datetime" || a_col.colType == "timestamp" || a_col.colType == "time") && (a_col.dtPrec > 0)
		result.columnInfos = append(result.columnInfos, a_col)
	}

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err = adbConn.QueryContext(ctx, "select COLUMN_NAME  from INFORMATION_SCHEMA.STATISTICS WHERE table_schema = ? AND table_name = ?   and INDEX_NAME = 'PRIMARY' order by SEQ_IN_INDEX    ", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query INFORMATION_SCHEMA.STATISTICS  to get primary key info for %s.%s\n%s", dbName, tableName, q_err.Error())
	}
	for q_rows.Next() {
		var a_str string
		err := q_rows.Scan(&a_str)
		if err != nil {
			log.Print("can not scan primary key informations")
			log.Fatal(err.Error())
		}
		result.primaryKey = append(result.primaryKey, a_str)
	}

	// ---------------------------------------------------------------------------------
	result.fullName = fmt.Sprintf("`%s`.`%s`", result.dbName, result.tbName)
	result.cntCols = len(result.columnInfos)
	// ---------------------------------------------------------------------------------
	if result.onError == 0 {
		ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

		q_rows, q_err = adbConn.QueryContext(ctx, fmt.Sprintf("select 1 from %s limit 1", result.fullName))
		if q_err != nil {
			log.Fatalf("can not query the table %s for one row\n", result.fullName, q_err.Error())
		}
		for q_rows.Next() {
			var a_bigint uint64
			err := q_rows.Scan(&a_bigint)
			if err != nil {
				log.Printf("can not scan a simple value from %s", result.fullName)
				log.Fatal(err.Error())
			}
			result.isEmpty = false
		}
		// -------------------------------------------------------------------------
		ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

		q_rows, q_err = adbConn.QueryContext(ctx, "select count(*) from INFORMATION_SCHEMA.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?", dbName, tableName)
		if q_err != nil {
			log.Fatalf("can not query INFORMATION_SCHEMA.TRIGGERS to detect trigger for table %s (%s) ", result.fullName, q_err.Error())
		}
		for q_rows.Next() {
			var a_bigint uint64
			err := q_rows.Scan(&a_bigint)
			if err != nil {
				log.Printf("can not scan count from INFORMATION_SCHEMA.TRIGGERS for table %s", result.fullName)
				log.Fatal(err.Error())
			}
			result.withTrigger = a_bigint > 0
		}
		// -------------------------------------------------------------------------
	}
	// ---------------------------------------------------------------------------------
	return result, true
}

// ------------------------------------------------------------------------------------------
func GetPostgresBasicMetadataInfo(adbConn *sql.Conn, dbName string, tableName string) (MetadataTable, bool) {
	var result MetadataTable
	result.dbName = dbName
	result.tbName = tableName
	result.fakePrimaryKey = false
	result.onError = 0
	result.isEmpty = true
	result.withTrigger = false

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := adbConn.PingContext(ctx)
	if p_err != nil {
		log.Fatalf("can not ping\n%s", p_err.Error())
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err := adbConn.QueryContext(ctx, "select coalesce(pg_total_relation_size(c.oid),-1),coalesce(c.reltuples::bigint,-1),'UNKNOW',TABLE_TYPE from information_schema.tables is_t left join pg_class c on c.relname = is_t.table_name left join pg_namespace n on n.oid = c.relnamespace and n.nspname = is_t.table_schema  WHERE table_schema = $1 AND table_name = $2     ", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query information_schema.tables for %s.%s\n%s", dbName, tableName, q_err.Error())
	}
	typeTable := "DO_NOT_EXIST"
	for q_rows.Next() {
		err := q_rows.Scan(&result.sizeBytes, &result.cntRows, &result.storageEng, &typeTable)
		if err != nil {
			log.Printf("can not query information_schema.tables for %s.%s", dbName, tableName)
			log.Fatal(err.Error())
		}
		if typeTable != "BASE TABLE" {
			result.onError = result.onError | 8
		}
	}
	if typeTable == "DO_NOT_EXIST" {
		result.onError = result.onError | 16
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err = adbConn.QueryContext(ctx, "select COLUMN_NAME , DATA_TYPE,IS_NULLABLE,COALESCE(DATETIME_PRECISION,-9999),COALESCE(numeric_precision,-9999) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = $1 AND table_name = $2 order by ORDINAL_POSITION ", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query information_schema.columns for %s.%s\n%s", dbName, tableName, q_err.Error())
	}
	for q_rows.Next() {
		var a_col columnInfo
		var a_str string
		err := q_rows.Scan(&a_col.colName, &a_col.colType, &a_str, &a_col.dtPrec, &a_col.nuPrec)
		if err != nil {
			log.Print("can not scan columns informations")
			log.Fatal(err.Error())
		}
		a_col.isNullable = (a_str == "YES")
		a_col.isKindChar = (a_col.colType == "character varying")
		a_col.isKindBinary = (a_col.colType == "bytea")
		a_col.mustBeQuote = a_col.isKindChar || a_col.isKindBinary || (a_col.colType == "date" || a_col.colType == "timestamp without time zone" || a_col.colType == "time" || a_col.colType == "timestamp with time zone")
		a_col.isKindFloat = (a_col.colType == "real" || a_col.colType == "double precision")
		a_col.haveFract = (a_col.colType == "timestamp without time zone" || a_col.colType == "timestamp with time zone" || a_col.colType == "time") && (a_col.dtPrec > 0)
		result.columnInfos = append(result.columnInfos, a_col)
	}

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	q_rows, q_err = adbConn.QueryContext(ctx,
		"SELECT pg_attribute.attname FROM pg_index, pg_class, pg_attribute, pg_namespace  WHERE nspname = $1 AND pg_class.relname = $2 AND indrelid = pg_class.oid AND pg_class.relnamespace = pg_namespace.oid AND    pg_attribute.attrelid = pg_class.oid AND pg_attribute.attnum = any(pg_index.indkey)  AND indisprimary order by array_position(pg_index.indkey,pg_attribute.attnum) ",
		dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query INFORMATION_SCHEMA.STATISTICS  to get primary key info for %s.%s\n%s", dbName, tableName, q_err.Error())
	}
	for q_rows.Next() {
		var a_str string
		err := q_rows.Scan(&a_str)
		if err != nil {
			log.Print("can not scan primary key informations")
			log.Fatal(err.Error())
		}
		result.primaryKey = append(result.primaryKey, a_str)
	}

	// ---------------------------------------------------------------------------------
	result.fullName = fmt.Sprintf("%s.%s", result.dbName, result.tbName)
	result.cntCols = len(result.columnInfos)
	// ---------------------------------------------------------------------------------
	if result.onError == 0 {
		ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

		q_rows, q_err = adbConn.QueryContext(ctx, fmt.Sprintf("select 1 from %s limit 1", result.fullName))
		if q_err != nil {
			log.Fatalf("can not query the table %s for one row\n", result.fullName, q_err.Error())
		}
		for q_rows.Next() {
			var a_bigint uint64
			err := q_rows.Scan(&a_bigint)
			if err != nil {
				log.Printf("can not scan a simple value from %s", result.fullName)
				log.Fatal(err.Error())
			}
			result.isEmpty = false
		}
		// -------------------------------------------------------------------------
		ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

		q_rows, q_err = adbConn.QueryContext(ctx, "select count(*) from INFORMATION_SCHEMA.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = $1 AND EVENT_OBJECT_TABLE = $2", dbName, tableName)
		if q_err != nil {
			log.Fatalf("can not query INFORMATION_SCHEMA.TRIGGERS to detect trigger for table %s (%s) ", result.fullName, q_err.Error())
		}
		for q_rows.Next() {
			var a_bigint uint64
			err := q_rows.Scan(&a_bigint)
			if err != nil {
				log.Printf("can not scan count from INFORMATION_SCHEMA.TRIGGERS for table %s", result.fullName)
				log.Fatal(err.Error())
			}
			result.withTrigger = a_bigint > 0
		}
		// -------------------------------------------------------------------------
	}
	// ---------------------------------------------------------------------------------
	return result, true
}

// ------------------------------------------------------------------------------------------
func GetTableMetadataInfo(adbConn *sql.Conn, dbName string, tableName string, guessPk bool, dumpmode string, dumpinsertwithcol string, srcdriver string, dstdriver string) (MetadataTable, bool) {
	result, _ := GetMysqlBasicMetadataInfo(adbConn, dbName, tableName)

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err := adbConn.QueryContext(ctx, "select COLUMN_NAME,coalesce(CARDINALITY,0),INDEX_NAME  from INFORMATION_SCHEMA.STATISTICS WHERE  table_schema = ? and table_name = ? and INDEX_NAME != 'PRIMARY' order by INDEX_NAME,SEQ_IN_INDEX", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query INFORMATION_SCHEMA.STATISTICS  to get index infos for %s.%s\n%s", dbName, tableName, q_err.Error())
	}
	var idx_cur indexInfo
	for q_rows.Next() {
		var a_cardina int64
		var a_col_name string
		var a_idx_name string

		err := q_rows.Scan(&a_col_name, &a_cardina, &a_idx_name)
		if err != nil {
			log.Printf("can not scan indexes informations for %s.%s", dbName, tableName)
			log.Fatal(err.Error())
		}
		if len(idx_cur.idxName) != 0 && idx_cur.idxName != a_idx_name {
			result.Indexes = append(result.Indexes, idx_cur)
			idx_cur.columns = nil
		}
		if len(idx_cur.columns) > 0 {
			result.Indexes = append(result.Indexes, idx_cur)
		}
		idx_cur.idxName = a_idx_name
		idx_cur.cardinality = a_cardina
		for _, v := range result.columnInfos {
			if v.colName == a_col_name {
				idx_cur.columns = append(idx_cur.columns, v)
			}
		}
		if mode_debug {
			log.Printf("idx_cur %s", idx_cur)
		}
	}
	if len(idx_cur.idxName) != 0 {
		result.Indexes = append(result.Indexes, idx_cur)
	}

	if len(result.primaryKey) == 0 {
		if !guessPk {
			result.onError = result.onError | 1
		} else {
			log.Printf("table %s.%s has no primary key\n", dbName, tableName)
			if mode_debug {
				log.Printf("table info is :")
				log.Printf("t.dbName         : %s", result.dbName)
				log.Printf("t.tbName         : %s", result.tbName)
				log.Printf("t.cntRows        : %s", result.cntRows)
				log.Printf("t.sizeBytes      : %s", result.sizeBytes)
				log.Printf("t.storageEng     : %s", result.storageEng)
				var a_str string
				for _, v := range result.columnInfos {
					if len(a_str) == 0 {
						a_str = v.colName
					} else {
						a_str = a_str + " , " + v.colName
					}
				}
				log.Printf("t.columnInfos    : [ %s ] ", a_str)
				log.Printf("t.primaryKey     : %s", result.primaryKey)
				log.Printf("t.Indexes        : ")
				for _, i := range result.Indexes {
					var a_str string
					for _, v := range i.columns {
						if len(a_str) == 0 {
							a_str = v.colName
						} else {
							a_str = a_str + " , " + v.colName
						}
					}
					log.Printf("         -  %9d [ %s ] %s ", i.cardinality, a_str, i.idxName)
				}
				log.Printf("t.fakePrimaryKey : %s", result.fakePrimaryKey)
				log.Printf("t.onError        : %s", result.onError)
			}
			// -----------------------------------------------------------------
			//
			// result.Indexes contains a list of all indexes AND in case of multi-columns indexes ALL implicit indexes you can have
			//
			var max_cardinality int64
			max_cardinality = -1
			ix_pos := -1
			for i, ix := range result.Indexes {
				ix_have_null := false
				for _, colinfo := range ix.columns {
					ix_have_null = ix_have_null || colinfo.isNullable
				}
				if !ix_have_null && ix.cardinality > max_cardinality {
					ix_pos = i
					max_cardinality = ix.cardinality
				}
			}
			// -----------------------------------------------------------------
			if ix_pos == -1 {
				result.onError = result.onError | 2
			} else {
				// ---------------------------------------------------------
				if mode_debug {
					log.Printf("we choose index %d", ix_pos)
				}
				result.fakePrimaryKey = true
				for _, colinfo := range result.Indexes[ix_pos].columns {
					result.primaryKey = append(result.primaryKey, colinfo.colName)
				}
				// ---------------------------------------------------------
			}
		}
	}
	result.cntPkCols = len(result.primaryKey)
	// ---------------------------------------------------------------------------------
	enumPkCols := make([]string, 0)
	for _, v := range result.columnInfos {
		if v.colType == "enum" {
			for _, pk := range result.primaryKey {
				if pk == v.colName {
					enumPkCols = append(enumPkCols, pk)
				}
			}
		}
	}
	if mode_debug {
		if len(enumPkCols) > 0 {
			log.Printf(" for table %s we have enum in pk for column %s", result.fullName, enumPkCols)
		}
	}
	// ---------------------------
	sql_cond_lower_pk, qry_indices_lo_bound := generatePredicat(result.primaryKey, true, enumPkCols)
	sql_cond_upper_pk, qry_indices_up_bound := generatePredicat(result.primaryKey, false, enumPkCols)
	sql_cond_equal_pk, qry_indices_equality := generateEqualityPredicat(result.primaryKey, enumPkCols)
	// ---------------------------
	result.listColsSQL = generateListCols4Sql(result.columnInfos, true)
	result.listColsPkSQL = generateListPkCols4Sql(result.primaryKey, "")
	result.listColsPkFetchSQL = generateListPkColsFetch4Sql(result.primaryKey, enumPkCols)
	result.listColsPkOrderSQL = generateListPkCols4Sql(result.primaryKey, "")
	result.listColsPkOrderDescSQL = generateListPkCols4Sql(result.primaryKey, "desc")
	result.listColsCSV = generateListCols4Csv(result.columnInfos)
	// ---------------------------
	result.query_for_browser_first = fmt.Sprintf("select %s from %s order by %s limit 1 ", result.listColsPkFetchSQL, result.fullName, result.listColsPkOrderSQL)
	if result.fakePrimaryKey {
		result.query_for_browser_next = fmt.Sprintf("select %s,cast(@cnt as unsigned integer ) as _cnt_pkey from ( select %s,@cnt:=@cnt+1 from %s , ( select @cnt := 0 ) c where %s order by %s limit %%d ) e order by %s limit 1 ",
			result.listColsPkSQL, result.listColsPkFetchSQL, result.fullName, sql_cond_lower_pk, result.listColsPkOrderSQL, result.listColsPkOrderDescSQL)
	} else {
		result.query_for_browser_next = fmt.Sprintf("select %s                                      from ( select %s              from %s                          where %s order by %s limit %%d ) e order by %s limit 1 ",
			result.listColsPkSQL, result.listColsPkFetchSQL, result.fullName, sql_cond_lower_pk, result.listColsPkOrderSQL, result.listColsPkOrderDescSQL)
	}
	result.param_indices_browser_next_qry = qry_indices_lo_bound
	// ---------------------------
	result.query_for_reader_equality = fmt.Sprintf("/* paradump */ select %s from %s where ( %s )           ", result.listColsSQL, result.fullName, sql_cond_equal_pk)
	result.param_indices_equality_qry = qry_indices_equality
	// ---------------------------
	result.query_for_reader_interval = fmt.Sprintf("/* paradump */ select %s from %s where ( %s ) and ( %s) ", result.listColsSQL, result.fullName, sql_cond_lower_pk, sql_cond_upper_pk)
	result.param_indices_interval_lo_qry = qry_indices_lo_bound
	result.param_indices_interval_up_qry = qry_indices_up_bound
	// ---------------------------
	if dumpmode == "cpy" {
		if dstdriver == "postgres" {
			result.query_for_insert = fmt.Sprintf("INSERT INTO %s.%s(%s) VALUES (", result.dbName, result.tbName, generateListCols4Sql(result.columnInfos, false))
		} else {
			result.query_for_insert = fmt.Sprintf("INSERT INTO %s(%s) VALUES (", result.fullName, result.listColsSQL)
		}
	} else {
		if dumpinsertwithcol == "full" {
			result.query_for_insert = fmt.Sprintf("INSERT INTO `%s`(%s) VALUES (", result.tbName, result.listColsSQL)
		} else {
			result.query_for_insert = fmt.Sprintf("INSERT INTO `%s` VALUES (", result.tbName)
		}
	}
	// ---------------------------
	if mode_debug {
		if len(enumPkCols) > 0 {
			log.Printf(" for table %s we need to apdat query because of enum in pk.\n%s", result.fullName, result.query_for_browser_first)
			log.Printf(" for table %s we need to apdat query because of enum in pk.\n%s", result.fullName, result.query_for_browser_next)
		}
	}
	// ---------------------------------------------------------------------------------
	return result, true
}

// ------------------------------------------------------------------------------------------
func GetListTables(adbConn *sql.Conn, dbNames []string, tab2exclude []string) []aTable {
	var result []aTable
	for _, v := range dbNames {
		result = append(result, GetListTablesBySchema(adbConn, v, tab2exclude)[:]...)
	}
	return result
}

// ------------------------------------------------------------------------------------------
func GetListTablesBySchema(adbConn *sql.Conn, dbName string, tab2exclude []string) []aTable {
	var result []aTable

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := adbConn.PingContext(ctx)
	if p_err != nil {
		log.Fatalf("can not ping\n%s", p_err.Error())
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	q_rows, d_err := adbConn.QueryContext(ctx, "SELECT count(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ? ", dbName)
	if d_err != nil {
		log.Fatalf("can not query database from  INFORMATION_SCHEMA.SCHEMATA for %s\n%s", dbName, d_err.Error())
	}
	for q_rows.Next() {
		var a_int int
		err := q_rows.Scan(&a_int)
		if err != nil {
			log.Print("can not scan database informations")
			log.Fatal(err.Error())
		}
		if a_int == 0 {
			log.Fatalf(" database '%s' does not exists\n", dbName)
		}
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	q_rows, q_err := adbConn.QueryContext(ctx, "select TABLE_name from information_schema.tables WHERE table_schema = ? and TABLE_TYPE='BASE TABLE' order by table_name ", dbName)
	if q_err != nil {
		log.Fatalf("can not list tables from  information_schema.tables for %s\n%s", dbName, q_err.Error())
	}
	for q_rows.Next() {
		var a_str string
		err := q_rows.Scan(&a_str)
		if err != nil {
			log.Print("can not scan table informations")
			log.Fatal(err.Error())
		}
		result = append(result, aTable{dbName: dbName, tbName: a_str})
	}
	for _, patexc := range tab2exclude {
		var result_flt []aTable
		for _, tab := range result {
			if strings.Index(tab.dbName+"."+tab.tbName, patexc) > 0 {
				log.Printf("%s.%s is excluded because match with %s", tab.dbName, tab.tbName, patexc)
			} else {
				result_flt = append(result_flt, tab)
			}
		}
		result = result_flt
	}
	return result
}

// ------------------------------------------------------------------------------------------
func GetMetadataInfo4Tables(adbConn *sql.Conn, tableNames []aTable, guessPk bool, dumpmode string, dumpinsertwithcol string, srcdriver string, dstdriver string) ([]MetadataTable, bool) {
	var result []MetadataTable
	for j := 0; j < len(tableNames); j++ {
		info, _ := GetTableMetadataInfo(adbConn, tableNames[j].dbName, tableNames[j].tbName, guessPk, dumpmode, dumpinsertwithcol, srcdriver, dstdriver)
		result = append(result, info)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].fullName < result[j].fullName })
	log.Printf("-------------------")
	cnt := 0
	for i, v := range result {
		if v.onError&1 == 1 && !(v.onError&16 == 16) {
			log.Printf("table %s.%s has no primary key\n", v.dbName, v.tbName)
			log.Print("you may want to use -guessprimarykey\n")
			cnt++
		}
		if v.onError&2 == 2 {
			log.Printf("table %s.%s has no alternative indexes to use as a primary key\n", v.dbName, v.tbName)
			cnt++
		}
		if v.onError&4 == 4 {
			log.Printf("table %s.%s is not a innodb table\n", v.dbName, v.tbName)
			cnt++
		}
		if v.onError&8 == 8 {
			log.Printf("table %s.%s is not a regular table\n", v.dbName, v.tbName)
			cnt++
		}
		if v.onError&16 == 16 {
			log.Printf("table %s.%s does not exists\n", v.dbName, v.tbName)
			cnt++
		}
		if i-1 >= 0 && v.onError&16 == 0 && v.fullName == result[i-1].fullName {
			log.Printf("table %s.%s is referenced twice\n", v.dbName, v.tbName)
			cnt++
		}
	}
	if cnt > 0 {
		log.Fatalf("too many ERRORS")
	}
	sort.Slice(result, func(i, j int) bool { return result[i].sizeBytes > result[j].sizeBytes })
	return result, true
}

// ------------------------------------------------------------------------------------------
func CheckTableOnDestination(driver string, adbConn *sql.Conn, a_table MetadataTable) (string, bool, int) {
	var dstinfo MetadataTable
	if driver == "mysql" {
		dstinfo, _ = GetMysqlBasicMetadataInfo(adbConn, a_table.dbName, a_table.tbName)
	} else {
		dstinfo, _ = GetPostgresBasicMetadataInfo(adbConn, a_table.dbName, a_table.tbName)
	}
	res := reflect.DeepEqual(a_table.columnInfos, dstinfo.columnInfos)
	err_msg := ""
	cnt_empty := 0
	if !res {
		err_msg = fmt.Sprintf("columns definitions are not identical for %s", a_table.fullName)
	}
	if driver != "postgres" {
		if dstinfo.withTrigger {
			err_msg = err_msg + fmt.Sprintf(" / table %s have triggers", a_table.fullName)
		}
	} else {
		if mode_debug {
			log.Printf("Table %s on destination will be populate without firing triggers", a_table.fullName)
		}
	}
	if !dstinfo.isEmpty {
		err_msg = err_msg + fmt.Sprintf(" / table %s is not empty", a_table.fullName)
		cnt_empty++
	}
	return err_msg, err_msg != "", cnt_empty
}

// ------------------------------------------------------------------------------------------
//
// will fetch DDL informations on destination database , and compare table definitions
func CheckTablesOnDestination(srcdriver string, dstdriver string, adbConn *sql.Conn, infTables []MetadataTable) {
	cnterr := 0
	cntempty := 0
	for n := range infTables {
		msg, err, notempty := CheckTableOnDestination(dstdriver, adbConn, infTables[n])
		if err {
			log.Printf("issue with table %s.%s on destination", infTables[n].dbName, infTables[n].tbName)
			log.Printf("%s", msg)
			cnterr++
		}
		cntempty = cntempty + notempty
	}
	if srcdriver != dstdriver && cntempty == 0 {
		log.Printf("WARNING source is on %s and destination is on %s , i will assume that will work.", srcdriver, dstdriver)
	} else {
		if cnterr > 0 {
			log.Fatalf("too many ERRORS")
		}
	}
}

// ------------------------------------------------------------------------------------------
type tablechunk struct {
	table_id        int
	chunk_id        int64
	is_done         bool
	begin_val       []string
	end_val         []string
	begin_equal_end bool
}

// ------------------------------------------------------------------------------------------
type colchunk struct {
	kind int
	val  *string
}

type rowchunk struct {
	cols []sql.NullString
}

type datachunk struct {
	table_id int
	usedlen  int
	chunk_id int64
	rows     []*rowchunk
}

// ------------------------------------------------------------------------------------------
type insertchunk struct {
	table_id int
	chunk_id int64
	sql      *string
}

// ------------------------------------------------------------------------------------------
func generateValuesForPredicat(indices []int, boundValues []string) []any {
	sql_vals := make([]any, 0)
	for _, v := range indices {
		sql_vals = append(sql_vals, boundValues[v])
	}
	return sql_vals
}

// ------------------------------------------------------------------------------------------
// lower bound is inclusive
// upper bound is exclusive
func generatePredicat(pkeyCols []string, lowerbound bool, enumCols []string) (string, []int) {
	var sql_pred string
	sql_vals_indices := make([]int, 0)
	ncolpkey := len(pkeyCols) - 1
	op_1 := " <  "
	op_o := " <  "
	if lowerbound {
		op_1 = " >=  "
		op_o = " >   "
	}
	sql_pred = " ( "
	for i := range pkeyCols {
		col_is_enum := false
		for e := range enumCols {
			if enumCols[e] == pkeyCols[i] {
				col_is_enum = true
				break
			}
		}
		place_holder := "?"
		if col_is_enum {
			place_holder = "cast(? as unsigned integer)"
		}
		if ncolpkey == i {
			sql_pred = sql_pred + fmt.Sprintf(" ( `%s` %s %s ) ", pkeyCols[i], op_1, place_holder)
		} else {
			sql_pred = sql_pred + fmt.Sprintf(" ( `%s` %s %s ) ", pkeyCols[i], op_o, place_holder)
		}
		sql_vals_indices = append(sql_vals_indices, i)
		if ncolpkey > 0 {
			for j := 0; j < ncolpkey; j++ {
				if j < i {
					col_is_enum := false
					for e := range enumCols {
						if enumCols[e] == pkeyCols[j] {
							col_is_enum = true
							break
						}
					}
					place_holder := "?"
					if col_is_enum {
						place_holder = "cast(? as unsigned integer)"
					}

					sql_pred = sql_pred + fmt.Sprintf(" and ( `%s` = %s ) ", pkeyCols[j], place_holder)
					sql_vals_indices = append(sql_vals_indices, j)
				}
			}
		}
		if i < ncolpkey {
			sql_pred = sql_pred + " ) or ( "
		}
	}
	sql_pred = sql_pred + " ) "
	return sql_pred, sql_vals_indices
}

// ------------------------------------------------------------------------------------------
func generateEqualityPredicat(pkeyCols []string, enumCols []string) (string, []int) {
	var sql_pred string
	sql_vals_indices := make([]int, 0)
	sql_pred = " ( "
	for i := range pkeyCols {
		if i != 0 {
			sql_pred = sql_pred + " and "
		}
		col_is_enum := false
		for e := range enumCols {
			if enumCols[e] == pkeyCols[i] {
				col_is_enum = true
				break
			}
		}
		place_holder := "?"
		if col_is_enum {
			place_holder = " cast( ? as unsigned integer ) "
		}
		sql_pred = sql_pred + fmt.Sprintf(" ( `%s` = %s ) ", pkeyCols[i], place_holder)
		sql_vals_indices = append(sql_vals_indices, i)
	}
	sql_pred = sql_pred + " ) "
	return sql_pred, sql_vals_indices
}

// ------------------------------------------------------------------------------------------
func tableChunkBrowser(adbConn *sql.Conn, id int, tableidstoscan chan int, tableInfos []MetadataTable, chunk2read chan tablechunk, sizeofchunk_init int64) {
	if mode_debug {
		log.Printf("tableChunkBrowser [%02d] start\n", id)
	}
	var sizeofchunk int64
	var must_prepare_query bool

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := adbConn.PingContext(ctx)
	if p_err != nil {
		log.Fatal("can not ping")
	}
	format_cnt_table := " %0" + fmt.Sprintf("%d", len(fmt.Sprintf("%d", len(tableInfos)))) + "d "
	for {
		j := <-tableidstoscan
		if j == -1 {
			break
		}
		sizeofchunk = sizeofchunk_init
		if mode_debug {
			log.Printf("table %s size pk %d query :  %s \n", tableInfos[j].fullName, tableInfos[j].cntPkCols, tableInfos[j].query_for_browser_first)
		}
		ctx, _ = context.WithTimeout(context.Background(), 16*time.Second)
		q_rows, q_err := adbConn.QueryContext(ctx, tableInfos[j].query_for_browser_first)
		if q_err != nil {
			log.Fatalf("can not query table %s to get first pk aka ( %s )\n%s", tableInfos[j].fullName, tableInfos[j].listColsPkSQL, q_err.Error())
		}
		a_sql_row := make([]*sql.NullString, tableInfos[j].cntPkCols)
		var pk_cnt int64
		ptrs := make([]any, tableInfos[j].cntPkCols)
		var ptrs_nextpk []any
		if tableInfos[j].fakePrimaryKey {
			ptrs_nextpk = make([]any, tableInfos[j].cntPkCols+1)
		} else {
			ptrs_nextpk = make([]any, tableInfos[j].cntPkCols)
		}
		for i := range a_sql_row {
			ptrs[i] = &a_sql_row[i]
			ptrs_nextpk[i] = &a_sql_row[i]
		}
		if tableInfos[j].fakePrimaryKey {
			ptrs_nextpk[tableInfos[j].cntPkCols] = &pk_cnt
		} else {
			pk_cnt = -1
		}
		row_cnt := 0
		for q_rows.Next() {
			err := q_rows.Scan(ptrs...)
			if err != nil {
				log.Printf("can not scan result for table %s \n", tableInfos[j].fullName)
				log.Fatal(err.Error())
			}
			row_cnt++
		}
		if row_cnt == 0 {
			log.Printf("table["+format_cnt_table+"] %s is empty \n", j, tableInfos[j].fullName)
			continue
		}
		start_pk_row := make([]string, tableInfos[j].cntPkCols)
		for n, value := range a_sql_row {
			start_pk_row[n] = value.String
		}
		log.Printf("table["+format_cnt_table+"] %s first pk ( %s ) - start scan pk %s\n", j, tableInfos[j].fullName, tableInfos[j].listColsPkSQL, start_pk_row)
		// --------------------------------------------------------------------------
		var the_finish_query string
		var prepare_finish_query *sql.Stmt
		var p_err error
		var end_pk_row []string
		var chunk_id int64 = 100000000 * (int64(j) + 1)
		// --------------------------------------------------------------------------
		end_pk_row = start_pk_row
		sizeofchunk = math.MaxInt
		// --------------------------------------------------------------------------
		for {
			start_pk_row = end_pk_row
			begin_equal_end := false
			if sizeofchunk > sizeofchunk_init {
				must_prepare_query = true
				sizeofchunk = sizeofchunk_init
			}
			for {
				// ----------------------------------------------------------
				if must_prepare_query {
					the_finish_query = fmt.Sprintf(tableInfos[j].query_for_browser_next, sizeofchunk)
					if prepare_finish_query != nil {
						_ = prepare_finish_query.Close()
					}
					ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
					prepare_finish_query, p_err = adbConn.PrepareContext(ctx, the_finish_query)
					if p_err != nil {
						log.Fatalf("can not prepare to get next pk ( %s )\n%s", the_finish_query, p_err.Error())
					}
					must_prepare_query = false
				}
				// ----------------------------------------------------------
				sql_vals_pk := generateValuesForPredicat(tableInfos[j].param_indices_browser_next_qry, start_pk_row)
				q_rows, q_err = prepare_finish_query.Query(sql_vals_pk...)
				if q_err != nil {
					log.Fatalf("can not query table %s to get next pk aka ( %s )\n%s", tableInfos[j].fullName, tableInfos[j].listColsPkSQL, q_err.Error())
				}
				for q_rows.Next() {
					err := q_rows.Scan(ptrs_nextpk...)
					if err != nil {
						log.Printf("can not scan result for table %s doing get next pk ( len ptrs_nextpk %d ) ( len sql_vals_pk %d) \n", tableInfos[j].fullName, len(ptrs_nextpk), len(sql_vals_pk))
						log.Printf("query for nextpk is %s", the_finish_query)
						log.Fatal(err.Error())
					}
				}
				end_pk_row = make([]string, tableInfos[j].cntPkCols)
				for n := range a_sql_row {
					end_pk_row[n] = a_sql_row[n].String
				}
				if mode_debug {
					log.Printf("tableChunkBrowser: table %s query :  %s \n with %d params\nval for query: %s\nresult: %s\n", tableInfos[j].fullName, the_finish_query, len(sql_vals_pk), sql_vals_pk, end_pk_row)
					log.Printf("tableChunkBrowser: table %s pk interval : %s -> %s\n", tableInfos[j].fullName, start_pk_row, end_pk_row)
					log.Printf("tableChunkBrowser: %s\n", reflect.DeepEqual(start_pk_row, end_pk_row))
				}
				begin_equal_end = reflect.DeepEqual(start_pk_row, end_pk_row)
				if tableInfos[j].fakePrimaryKey && begin_equal_end && pk_cnt >= sizeofchunk {
					sizeofchunk = sizeofchunk * 15 / 10
					must_prepare_query = true
				} else {
					break
				}
				// ----------------------------------------------------------
			}
			// ------------------------------------------------------------------
			chunk_id++
			var a_chunk tablechunk
			a_chunk.table_id = j
			a_chunk.chunk_id = chunk_id
			a_chunk.begin_val = start_pk_row
			a_chunk.end_val = end_pk_row
			a_chunk.begin_equal_end = begin_equal_end
			a_chunk.is_done = false
			chunk2read <- a_chunk
			// ------------------------------------------------------------------
			if begin_equal_end {
				break
			}
		}
		log.Printf("table["+format_cnt_table+"] %s scan is done , pk col ( %s ) scan size pk %d last pk %s\n", j, tableInfos[j].fullName, tableInfos[j].listColsPkSQL, pk_cnt, end_pk_row)
		// --------------------------------------------------------------------------
		if prepare_finish_query != nil {
			_ = prepare_finish_query.Close()
		}
		// --------------------------------------------------------------------------
	}
	if mode_debug {
		log.Printf("tableChunkBrowser [%02d] finish\b", id)
	}
	// ----------------------------------------------------------------------------------
}

// ------------------------------------------------------------------------------------------
func ChunkReaderDumpHeader(dumpmode string, cur_iow io.Writer, sql_tab_cols string) {
	if dumpmode == "sql" {
		io.WriteString(cur_iow, "SET NAMES utf8mb4;\n")
		io.WriteString(cur_iow, "SET TIME_ZONE='+00:00';\n")
	}
	if dumpmode == "csv" {
		io.WriteString(cur_iow, fmt.Sprintf("%s\n", sql_tab_cols))
	}
}

// ------------------------------------------------------------------------------------------
func ChunkReaderDumpProcess(threadid int, q_rows *sql.Rows, a_table_info *MetadataTable, tab_id int, chk_id int64, insert_size int, chan2gen chan datachunk) {
	// --------------------------------------------------------------------------
	var a_dta_chunk datachunk
	a_dta_chunk = datachunk{usedlen: 0, chunk_id: chk_id, table_id: tab_id, rows: make([]*rowchunk, insert_size)}
	row_cnt := 0
	if mode_debug {
		log.Printf("ChunkReaderDumpProcess [%02d] table %03d chunk %12d \n", threadid, tab_id, chk_id)
	}
	ptrs := make([]any, a_table_info.cntCols)
	for q_rows.Next() {
		var a_simple_row rowchunk
		a_simple_row.cols = make([]sql.NullString, a_table_info.cntCols)
		// ------------------------------------------------------------------
		for i := 0; i < a_table_info.cntCols; i++ {
			ptrs[i] = &a_simple_row.cols[i]
		}
		// ------------------------------------------------------------------
		err := q_rows.Scan(ptrs...)
		if err != nil {
			log.Printf("can not scan %s ( already scan %d ) , len(ptrs) = %d , cols %s ", a_table_info.tbName, row_cnt, len(ptrs))
			log.Fatal(err)
		}
		// ------------------------------------------------------------------
		a_dta_chunk.rows[row_cnt] = &a_simple_row
		row_cnt++
		// ------------------------------------------------------------------
		if row_cnt >= insert_size {
			a_dta_chunk.usedlen = row_cnt
			chan2gen <- a_dta_chunk
			row_cnt = 0
			a_dta_chunk = datachunk{usedlen: 0, table_id: tab_id, chunk_id: chk_id, rows: make([]*rowchunk, insert_size)}
		}
	}
	if row_cnt > 0 {
		a_dta_chunk.usedlen = row_cnt
		chan2gen <- a_dta_chunk
	}
}

// ------------------------------------------------------------------------------------------
type cacheTableChunkReader struct {
	table_id               int
	lastusagecnt           int
	fullname               string
	indices_lo_pk          []int
	indices_up_pk          []int
	indices_equal_pk       []int
	interval_query         string
	equality_query         string
	interval_prepared_stmt *sql.Stmt
	equality_prepared_stmt *sql.Stmt
}

// ------------------------------------------------------------------------------------------
func tableChunkReader(chunk2read chan tablechunk, chan2generator chan datachunk, adbConn *sql.Conn, tableInfos []MetadataTable, id int, insert_size int, cntBrowser int) {
	if mode_debug {
		log.Printf("tableChunkReader[%02d] start with insert size %d\n", id, insert_size)
	}
	var cntreadchunk int = 0
	var last_hit int = 0
	var cache_hit int = 0
	var cache_miss int = 0

	var tabReadingVars []*cacheTableChunkReader

	var last_table *cacheTableChunkReader

	tabReadingVars = make([]*cacheTableChunkReader, cntBrowser+1)

	for {
		a_chunk := <-chunk2read
		if a_chunk.is_done {
			break
		}
		cntreadchunk++

		if last_table == nil || last_table.table_id != a_chunk.table_id {
			// ------------------------------------------------------------------
			var tab_found int = -1
			var empty_slot int = -1
			var lru_slot int = -1
			var lru_val int = math.MaxInt
			for n := range tabReadingVars {
				if tabReadingVars[n] == nil {
					empty_slot = n
				} else if tabReadingVars[n].table_id == a_chunk.table_id {
					tab_found = n
					break
				} else {
					if tabReadingVars[n].lastusagecnt < lru_val {
						lru_val = tabReadingVars[n].lastusagecnt
						lru_slot = n
					}
				}
			}
			if tab_found == -1 {
				cache_miss++
				// ----------------------------------------------------------
				if empty_slot == -1 {
					tabReadingVars[lru_slot].interval_prepared_stmt.Close()
					tabReadingVars[lru_slot].equality_prepared_stmt.Close()
					tabReadingVars[lru_slot] = nil
					empty_slot = lru_slot
				}
				// ----------------------------------------------------------
				var new_info cacheTableChunkReader

				new_info.table_id = a_chunk.table_id
				new_info.lastusagecnt = cntreadchunk

				new_info.fullname = tableInfos[a_chunk.table_id].fullName
				new_info.interval_query = tableInfos[a_chunk.table_id].query_for_reader_interval
				new_info.equality_query = tableInfos[a_chunk.table_id].query_for_reader_equality
				new_info.indices_lo_pk = tableInfos[a_chunk.table_id].param_indices_interval_lo_qry
				new_info.indices_up_pk = tableInfos[a_chunk.table_id].param_indices_interval_up_qry
				new_info.indices_equal_pk = tableInfos[a_chunk.table_id].param_indices_equality_qry
				// ------------------------------------------------------------------
				ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
				var p_err error
				new_info.interval_prepared_stmt, p_err = adbConn.PrepareContext(ctx, new_info.interval_query)
				if p_err != nil {
					log.Printf("tableChunkReader[%02d] finish cntreadchunk:%9d last_hit: %9d cache_hit: %9d cache_miss:%9d\n", id, cntreadchunk, last_hit, cache_hit, cache_miss)
					log.Fatalf("can not prepare to read a chunk ( %s )\n%s", new_info.interval_query, p_err.Error())
				}
				ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
				new_info.equality_prepared_stmt, p_err = adbConn.PrepareContext(ctx, new_info.equality_query)
				if p_err != nil {
					log.Printf("tableChunkReader[%02d] finish cntreadchunk:%9d last_hit: %9d cache_hit: %9d cache_miss:%9d\n", id, cntreadchunk, last_hit, cache_hit, cache_miss)
					log.Fatalf("can not prepare to read a chunk of one pk ( %s )\n%s", new_info.equality_query, p_err.Error())
				}
				// ------------------------------------------------------------------
				tabReadingVars[empty_slot] = &new_info
				last_table = &new_info
			} else {
				cache_hit++
				last_table = tabReadingVars[tab_found]
				last_table.lastusagecnt = cntreadchunk
			}
		} else {
			last_hit++
		}
		// --------------------------------------------------------------------------
		if mode_debug {
			log.Printf("tableChunkReader[%02d] table %03d chunk %12d %s -> %s\n", id, a_chunk.table_id, a_chunk.chunk_id, a_chunk.begin_val, a_chunk.end_val)
		}
		// --------------------------------------------------------------------------
		var sql_vals_pk []any
		var the_query *string
		var prepared_query *sql.Stmt
		if a_chunk.begin_equal_end {
			sql_vals_pk = generateValuesForPredicat(last_table.indices_equal_pk, a_chunk.begin_val)
			the_query = &last_table.equality_query
			prepared_query = last_table.equality_prepared_stmt
		} else {
			sql_vals_pk = generateValuesForPredicat(last_table.indices_lo_pk, a_chunk.begin_val)
			sql_vals_pk = append(sql_vals_pk, generateValuesForPredicat(last_table.indices_up_pk, a_chunk.end_val)...)
			the_query = &last_table.interval_query
			prepared_query = last_table.interval_prepared_stmt
		}
		q_rows, q_err := prepared_query.Query(sql_vals_pk...)
		if q_err != nil {
			log.Printf("table %s chunk id: %12d chunk query :  %s \n with %d params\nval for query: %s\n", last_table.fullname, a_chunk.chunk_id, *the_query, len(sql_vals_pk), sql_vals_pk)
			log.Printf("ind lo: %s  ind up: %s", last_table.indices_lo_pk, last_table.indices_up_pk)
			log.Fatalf("can not query table %s to read the chunks\n%s", last_table.fullname, sql_vals_pk, q_err.Error())
		}
		if mode_debug {
			log.Printf("table %s chunk id: %12d chunk query :  %s \n with %d params\nval for query: %s\n", last_table.fullname, a_chunk.chunk_id, *the_query, len(sql_vals_pk), sql_vals_pk)
			log.Printf("ind lo: %s  ind up: %s", last_table.indices_lo_pk, last_table.indices_up_pk)
		}
		// --------------------------------------------------------------------------
		ChunkReaderDumpProcess(id, q_rows, &tableInfos[last_table.table_id], last_table.table_id, a_chunk.chunk_id, insert_size, chan2generator)
		// --------------------------------------------------------------------------
		if mode_debug {
			log.Printf("table %s chunk query :  %s \n with %d params\nval for query: %s\n", last_table.fullname, *the_query, len(sql_vals_pk), sql_vals_pk)
		}
		// --------------------------------------------------------------------------
	}
	// ----------------------------------------------------------------------------------
	for n := range tabReadingVars {
		if tabReadingVars[n] != nil {
			tabReadingVars[n].interval_prepared_stmt.Close()
			tabReadingVars[n].equality_prepared_stmt.Close()
		}
	}
	// ----------------------------------------------------------------------------------
	if mode_debug {
		log.Printf("tableChunkReader[%02d] finish cntreadchunk:%9d last_hit: %9d cache_hit: %9d cache_miss:%9d\n", id, cntreadchunk, last_hit, cache_hit, cache_miss)
		log.Printf("tableChunkReader[%02d] finish\n", id)
	}
}

// ------------------------------------------------------------------------------------------
func generateListPkColsFetch4Sql(col_pk []string, enumCols []string) string {
	if len(col_pk) == 0 {
		return ""
	}
	a_str := ""
	for ctab := 0; ctab < len(col_pk); ctab++ {
		col_is_enum := false
		for e := range enumCols {
			if enumCols[e] == col_pk[ctab] {
				col_is_enum = true
				break
			}
		}
		if ctab > 0 {
			a_str = a_str + ","
		}
		if col_is_enum {
			a_str = a_str + " cast(`" + col_pk[ctab] + "` as unsigned integer ) as `" + col_pk[ctab] + "`"
		} else {
			a_str = a_str + " `" + col_pk[ctab] + "` "
		}
	}
	return a_str
}

// ------------------------------------------------------------------------------------------
func generateListPkCols4Sql(col_pk []string, dml_desc string) string {
	if len(col_pk) == 0 {
		return ""
	}
	a_str := "`" + col_pk[0] + "` " + dml_desc
	for ctab := 1; ctab < len(col_pk); ctab++ {
		a_str = a_str + ",`" + col_pk[ctab] + "` " + dml_desc
	}
	return a_str
}

// ------------------------------------------------------------------------------------------
func generateListCols4Sql(col_inf []columnInfo, backtick bool) string {
	if len(col_inf) == 0 {
		return ""
	}
	var a_str string
	if backtick {
		a_str = "`" + col_inf[0].colName + "`"
		for ctab := 1; ctab < len(col_inf); ctab++ {
			a_str = a_str + ",`" + col_inf[ctab].colName + "`"
		}
	} else {
		a_str = col_inf[0].colName
		for ctab := 1; ctab < len(col_inf); ctab++ {
			a_str = a_str + "," + col_inf[ctab].colName
		}
	}
	return a_str
}

// ------------------------------------------------------------------------------------------
func generateListCols4Csv(col_inf []columnInfo) string {
	if len(col_inf) == 0 {
		return ""
	}
	a_str := col_inf[0].colName
	for ctab := 1; ctab < len(col_inf); ctab++ {
		a_str = a_str + "," + col_inf[ctab].colName
	}
	return a_str
}

// ------------------------------------------------------------------------------------------
//
// replicate quoting string from  mysql
//
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L823-L932
//
// https://en.wikipedia.org/wiki/UTF-8#Encoding
//
// https://stackoverflow.com/questions/34861479/how-to-detect-when-bytes-cant-be-converted-to-string-in-go
//
//	example https://go.dev/play/p/6yHonH0Mae
var quote_substitute_string_mysql = [256]uint8{
	//    1    2    3    4     5    6    7    8    9    A    B     C    D    E    F
	'0', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 'n', ' ', ' ', 'r', ' ', ' ', // 0x00-0x0F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 'Z', ' ', ' ', ' ', ' ', ' ', // 0x10-0x1F
	' ', ' ', '"', ' ', ' ', ' ', ' ', '\'', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x20-0x2F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x30-0x3F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x40-0x4F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', '\\', ' ', ' ', ' ', // 0x50-0x5F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x60-0x6F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x70-0x7F
	//    1    2    3    4     5    6    7    8    9    A    B     C    D    E    F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x80-0x8F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x90-0x9F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xA0-0xAF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xB0-0xBF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xC0-0xCF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xD0-0xDF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xE0-0xEF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xF0-0xFF
}

func needCopyForquoteStringMysql(s_ptr *string) (*string, int, int, byte) {
	s_len := len(*s_ptr)
	if s_len == 0 {
		return s_ptr, 0, -1, 0
	}
	b_pos := 0
	var new_char byte
	for {
		first_char := (*s_ptr)[b_pos]
		new_char = quote_substitute_string_mysql[first_char]
		if new_char != ' ' {
			return s_ptr, s_len, b_pos, new_char
		}
		b_pos++
		if b_pos == s_len {
			return s_ptr, s_len, -1, new_char
		}
	}
}

func quoteStringFromPosMysql(s_ptr *string, s_len int, b_pos int, new_char byte) (*string, int) {
	var new_str strings.Builder
	new_str.WriteString((*s_ptr)[:b_pos])
	new_str.WriteByte('\\')
	new_str.WriteByte(new_char)
	b_pos++
	for b_pos < s_len {
		first_char := (*s_ptr)[b_pos]
		new_char = quote_substitute_string_mysql[first_char]
		if new_char == ' ' {
			new_str.WriteByte(first_char)
		} else {
			new_str.WriteByte('\\')
			new_str.WriteByte(new_char)
		}
		b_pos++
	}
	n_str := new_str.String()
	return &n_str, len(n_str)
}

var quote_substitute_binary = [256]uint8{
	//    1    2    3    4    5    6     7    8    9    A    B     C    D    E    F
	'0', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 'n', ' ', ' ', 'r', ' ', ' ', // 0x00-0x0F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 'Z', ' ', ' ', ' ', ' ', ' ', // 0x10-0x1F
	' ', ' ', '"', ' ', ' ', ' ', ' ', '\'', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x20-0x2F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x30-0x3F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x40-0x4F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', '\\', ' ', ' ', ' ', // 0x50-0x5F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x60-0x6F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x70-0x7F
	//    1    2    3    4    5    6     7    8    9    A    B     C    D    E    F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x80-0x8F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x90-0x9F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xA0-0xAF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xB0-0xBF
	' ', ' ', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', // 0xC0-0xCF
	'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', // 0xD0-0xDF
	'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', // 0xE0-0xEF
	'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xF0-0xFF
}

func quoteBinary(s_ptr *string) (*string, int) {
	if s_ptr == nil || len(*s_ptr) == 0 {
		return s_ptr, 0
	}
	b_pos := 0
	need_a_copy := false
	s_len := len(*s_ptr)
	var new_char byte
stringLoop:
	for b_pos < s_len {
		first_char := (*s_ptr)[b_pos]
		new_char = quote_substitute_binary[first_char]
		switch new_char {
		case ' ':
			b_pos++
		case 'f':
			r, size := utf8.DecodeRuneInString((*s_ptr)[b_pos:])
			if r == utf8.RuneError {
				if first_char == 0xed && b_pos+2 < s_len && (*s_ptr)[b_pos+1] >= 0xa0 && (*s_ptr)[b_pos+1] <= 0xbf && (*s_ptr)[b_pos+2] >= 0x80 && (*s_ptr)[b_pos+2] <= 0xbf {
					b_pos++
					continue
				}
				need_a_copy = true
				break stringLoop
			}
			b_pos += size
		default:
			need_a_copy = true
			break stringLoop
		}
	}
	if !need_a_copy {
		return s_ptr, s_len
	}
	var new_str strings.Builder
	new_str.Grow(s_len * 2)
	new_str.WriteString((*s_ptr)[:b_pos])
	for b_pos < s_len {
		first_char := (*s_ptr)[b_pos]
		new_char = quote_substitute_binary[first_char]
		switch new_char {
		case ' ':
			new_str.WriteByte(first_char)
		case 'f':
			r, size := utf8.DecodeRuneInString((*s_ptr)[b_pos:])
			if r == utf8.RuneError {
				// ED  a0  80   => ED  bf  bf
				if first_char == 0xed && b_pos+2 < s_len && (*s_ptr)[b_pos+1] >= 0xa0 && (*s_ptr)[b_pos+1] <= 0xbf && (*s_ptr)[b_pos+2] >= 0x80 && (*s_ptr)[b_pos+2] <= 0xbf {
					new_str.WriteByte(first_char)
					new_str.WriteByte((*s_ptr)[b_pos+1])
					new_str.WriteByte((*s_ptr)[b_pos+2])
					b_pos += 3
					continue
				} else {
					new_str.WriteByte('\\')
					new_str.WriteByte(first_char)
				}
			} else {
				new_str.WriteRune(r)
				b_pos += size
				continue
			}
		default:
			new_str.WriteByte('\\')
			new_str.WriteByte(new_char)
		}
		b_pos++
	}
	n_str := new_str.String()
	return &n_str, len(n_str)
}

var quote_csv_string = [256]uint8{
	//    1    2    3    4    5    6    7    8    9    A    B    C    D    E    F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 'n', ' ', ' ', ' ', ' ', ' ', // 0x00-0x0F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x10-0x1F
	' ', ' ', '"', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ',', ' ', ' ', ' ', // 0x20-0x2F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x30-0x3F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x40-0x4F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x50-0x5F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x60-0x6F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x70-0x7F
	//    1    2    3    4     5    6    7    8    9    A    B   C    D    E    F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x80-0x8F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x90-0x9F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xA0-0xAF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xB0-0xBF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xC0-0xCF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xD0-0xDF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xE0-0xEF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xF0-0xFF
}

func needQuoteForCsv(s_ptr *string) bool {
	s_len := len(*s_ptr)
	if s_len == 0 {
		return false
	}
	b_pos := 0
	var new_char byte
	for {
		first_char := (*s_ptr)[b_pos]
		new_char = quote_csv_string[first_char]
		if new_char == ' ' {
			b_pos++
			if b_pos == s_len {
				return false
			}
		} else {
			break
		}
	}
	return true
}

// ------------------------------------------------------------------------------------------
// postgres : we need only to quote the single quote
var quote_substitute_string_postgres = [256]uint8{
	//    1    2    3    4     5    6      7    8    9       A    B       C       D    E    F
	'Z', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', '\x0A', ' ', ' ', '\x0D', ' ', ' ', // 0x00-0x0F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', '\x1A', ' ', ' ', ' ', ' ', ' ', // 0x10-0x1F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', '\x27', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x20-0x2F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x30-0x3F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x40-0x4F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', '\x5C', ' ', ' ', ' ', // 0x50-0x5F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x60-0x6F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x70-0x7F
	//    1    2    3    4     5    6    7    8    9    A    B     C    D    E    F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x80-0x8F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0x90-0x9F
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xA0-0xAF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xB0-0xBF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xC0-0xCF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xD0-0xDF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xE0-0xEF
	' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', // 0xF0-0xFF
}

const hextable = "0123456789abcdef"

func needCopyForquoteStringPostgres(s_ptr *string) (*string, int, int, byte) {
	s_len := len(*s_ptr)
	if s_len == 0 {
		return s_ptr, 0, -1, 0
	}
	b_pos := 0
	var new_char byte
	for {
		first_char := (*s_ptr)[b_pos]
		new_char = quote_substitute_string_postgres[first_char]
		if new_char != ' ' {
			return s_ptr, s_len, b_pos, new_char
		}
		b_pos++
		if b_pos == s_len {
			return s_ptr, s_len, -1, 0
		}
	}
}

func quoteStringFromPosPostgres(s_ptr *string, s_len int, b_pos int, new_char byte) (*string, int) {
	var new_str strings.Builder
	new_str.WriteString((*s_ptr)[:b_pos])
	if new_char != 'Z' {
		new_str.WriteString("\\x")
		new_str.WriteByte(hextable[new_char>>4])
		new_str.WriteByte(hextable[new_char&0x0f])
	}
	b_pos++
	for b_pos < s_len {
		first_char := (*s_ptr)[b_pos]
		new_char = quote_substitute_string_postgres[first_char]
		if new_char == ' ' {
			new_str.WriteByte(first_char)
		} else if new_char != 'Z' {
			new_str.WriteString("\\x")
			new_str.WriteByte(hextable[new_char>>4])
			new_str.WriteByte(hextable[new_char&0x0f])
		}
		b_pos++
	}
	n_str := new_str.String()
	return &n_str, len(n_str)
}

// ------------------------------------------------------------------------------------------
type cachedataChunkGenerator struct {
	table_id     int
	lastusagecnt int
	buf_arr      []colchunk
	init_size    int
	pad_row_size int
	tab_meta     *MetadataTable
}

// ------------------------------------------------------------------------------------------
func dataChunkGenerator(rowvalueschan chan datachunk, id int, tableInfos []MetadataTable, dumpmode string, sql2inject chan insertchunk, insert_size int, dst_driver string, cntBrowser int) {
	// ----------------------------------------------------------------------------------
	if dumpmode == "sql" || dumpmode == "cpy" {
		// ------------------------------------------------------------------
		// we use a array of string to generate the final sql insert
		//
		//   row 1 |   insert into (   | col1  | , |  col2 | , | ....   | coln
		//   row 2 |   ) , (           | col1  | , |  col2 | , | ....   | coln
		//   row 3 |   ) , (           | col1  | , |  col2 | , | ....   | coln
		//   row 4 |   ) , (           | col1  | , |  col2 | , | ....   | coln
		//   end   |   ) ; \n
		//
		//    4 rows with n cols => 4*n*2 + 1
		//
		//  echo row need 1           prefix
		//                cntCols     for value
		//                cntCols - 1 for coma
		//  and a final   suffix
		//
		//  total cells => insert_size x ( tab_meta.cntCols * 2 ) cells + 1
		//
		// ------------------------------------------------------------------
		nullStr := "NULL"
		betwStr := "),("
		endStr := ");\n"

		var cntgenechunk int = 0
		var last_hit int = 0
		var cache_hit int = 0
		var cache_miss int = 0

		var tabGenVars []*cachedataChunkGenerator
		var lastTable *cachedataChunkGenerator

		tabGenVars = make([]*cachedataChunkGenerator, cntBrowser+1)

		// ------------------------------------------------------------------
		for {
			a_dta_chunk := <-rowvalueschan
			if mode_debug {
				log.Printf("[%02d] dataChunkGenerator table %03d chunk %12d usedlen %5d", id, a_dta_chunk.table_id, a_dta_chunk.chunk_id, a_dta_chunk.usedlen)
			}
			if a_dta_chunk.table_id == -1 {
				break
			}
			cntgenechunk++

			if lastTable != nil && lastTable.table_id == a_dta_chunk.table_id {
				last_hit++
			} else {
				// --------------------------------------------------
				var tab_found int = -1
				var empty_slot int = -1
				var lru_slot int = -1
				var lru_val int = math.MaxInt
				for n := range tabGenVars {
					if tabGenVars[n] == nil {
						empty_slot = n
					} else if tabGenVars[n].table_id == a_dta_chunk.table_id {
						tab_found = n
						break
					} else {
						if tabGenVars[n].lastusagecnt < lru_val {
							lru_val = tabGenVars[n].lastusagecnt
							lru_slot = n
						}
					}
				}
				if tab_found > -1 {
					cache_hit++
					lastTable = tabGenVars[tab_found]
					lastTable.lastusagecnt = cntgenechunk
					cache_miss++
				} else {
					if empty_slot == -1 {
						empty_slot = lru_slot
					}
					var new_info cachedataChunkGenerator
					tabGenVars[empty_slot] = &new_info
					lastTable = &new_info
					// -----------------------------------------
					lastTable.table_id = a_dta_chunk.table_id
					// -----------------------------------------
					lastTable.tab_meta = &tableInfos[lastTable.table_id]
					// -----------------------------------------
					lastTable.buf_arr = make([]colchunk, insert_size*2*lastTable.tab_meta.cntCols+1)
					// -----------------------------------------
					u_ind := 0
					coma_str := ","
					pad_beg_size := 0
					pad_mid_size := 0 //  will be repeat =>  cnt rows -1
					pad_end_size := 0
					// -----------------------------------------
					// row 1
					lastTable.buf_arr[u_ind].val = &lastTable.tab_meta.query_for_insert
					lastTable.buf_arr[u_ind].kind = 0
					pad_beg_size += len(lastTable.tab_meta.query_for_insert)
					u_ind += 2
					for c := 1; c < lastTable.tab_meta.cntCols; c++ {
						lastTable.buf_arr[u_ind].val = &coma_str
						lastTable.buf_arr[u_ind].kind = 0
						pad_beg_size += 1
						u_ind += 2
					}
					if insert_size > 1 {
						// -----------------------------------------
						// row 2
						lastTable.buf_arr[u_ind].val = &betwStr
						lastTable.buf_arr[u_ind].kind = 0
						pad_mid_size += len(betwStr)
						u_ind += 2
						for c := 1; c < lastTable.tab_meta.cntCols; c++ {
							lastTable.buf_arr[u_ind].val = &coma_str
							lastTable.buf_arr[u_ind].kind = 0
							pad_mid_size += 1
							u_ind += 2
						}
						// -----------------------------------------
						for r := 2; r < insert_size; r++ {
							for c := 0; c < lastTable.tab_meta.cntCols; c++ {
								r_ind := (u_ind % (lastTable.tab_meta.cntCols * 2)) + lastTable.tab_meta.cntCols*2
								lastTable.buf_arr[u_ind].val = lastTable.buf_arr[r_ind].val
								lastTable.buf_arr[u_ind].kind = 0
								u_ind += 2
							}
						}
						// -----------------------------------------
					}
					lastTable.buf_arr[u_ind].val = &endStr
					lastTable.buf_arr[u_ind].kind = 0
					pad_end_size += len(endStr)
					// -----------------------------------------
					lastTable.init_size = pad_beg_size + pad_end_size
					lastTable.pad_row_size = pad_mid_size
				}
			}

			last_cell_pos := a_dta_chunk.usedlen * 2 * lastTable.tab_meta.cntCols
			b_siz := lastTable.init_size + (a_dta_chunk.usedlen-1)*lastTable.pad_row_size

			for n := 0; n < lastTable.tab_meta.cntCols; n++ {
				arr_ind := 1 + n*2 + (a_dta_chunk.usedlen-1)*2*lastTable.tab_meta.cntCols
				arr_ind_inc := 2 * lastTable.tab_meta.cntCols
				// -------------------------------------------------
				if lastTable.tab_meta.columnInfos[n].isKindBinary && dst_driver == "mysql" {
					for j := a_dta_chunk.usedlen - 1; j >= 0; j-- {
						cell := a_dta_chunk.rows[j].cols[n]
						if !cell.Valid {
							lastTable.buf_arr[arr_ind].kind = -1
							b_siz += 4
							arr_ind -= arr_ind_inc
							continue
						}
						s_ptr, cell_size := quoteBinary(&cell.String)
						lastTable.buf_arr[arr_ind].kind = 2
						lastTable.buf_arr[arr_ind].val = s_ptr
						// 7 for _binary and 1 for space and 2 for quotes
						b_siz += cell_size + 7 + 1 + 2
						arr_ind -= arr_ind_inc
					}
				} else if lastTable.tab_meta.columnInfos[n].isKindBinary && dst_driver == "postgres" {
					for j := a_dta_chunk.usedlen - 1; j >= 0; j-- {
						cell := a_dta_chunk.rows[j].cols[n]
						if !cell.Valid {
							lastTable.buf_arr[arr_ind].kind = -1
							b_siz += 4
							arr_ind -= arr_ind_inc
							continue
						}
						s_ptr := hex.EncodeToString([]byte(cell.String))
						cell_size := len(s_ptr)
						lastTable.buf_arr[arr_ind].kind = 2
						lastTable.buf_arr[arr_ind].val = &s_ptr
						// 7 for decode( and 2 for quotes and 7 for ,'hex')
						b_siz += cell_size + 7 + 2 + 7
						arr_ind -= arr_ind_inc
					}
				} else if lastTable.tab_meta.columnInfos[n].mustBeQuote && dst_driver == "mysql" {
					for j := a_dta_chunk.usedlen - 1; j >= 0; j-- {
						cell := a_dta_chunk.rows[j].cols[n]
						if !cell.Valid {
							lastTable.buf_arr[arr_ind].kind = -1
							b_siz += 4
							arr_ind -= arr_ind_inc
							continue
						}
						s_ptr, cell_size, need_quote, new_char := needCopyForquoteStringMysql(&cell.String)
						if need_quote != -1 {
							s_ptr, cell_size = quoteStringFromPosMysql(&cell.String, cell_size, need_quote, new_char)
						}
						b_siz += cell_size + 2
						lastTable.buf_arr[arr_ind].kind = 1
						lastTable.buf_arr[arr_ind].val = s_ptr
						arr_ind -= arr_ind_inc
					}
				} else if lastTable.tab_meta.columnInfos[n].mustBeQuote && dst_driver == "postgres" {
					for j := a_dta_chunk.usedlen - 1; j >= 0; j-- {
						cell := a_dta_chunk.rows[j].cols[n]
						if !cell.Valid {
							lastTable.buf_arr[arr_ind].kind = -1
							b_siz += 4
							arr_ind -= arr_ind_inc
							continue
						}
						s_ptr, cell_size, need_quote, new_char := needCopyForquoteStringPostgres(&cell.String)
						if need_quote != -1 {
							s_ptr, cell_size = quoteStringFromPosPostgres(&cell.String, cell_size, need_quote, new_char)
							lastTable.buf_arr[arr_ind].kind = 3
							b_siz += cell_size + 3
						} else {
							lastTable.buf_arr[arr_ind].kind = 1
							b_siz += cell_size + 2
						}
						lastTable.buf_arr[arr_ind].val = s_ptr
						arr_ind -= arr_ind_inc
					}
				} else if lastTable.tab_meta.columnInfos[n].isKindFloat {
					var f_prec uint = 24
					if lastTable.tab_meta.columnInfos[n].colType == "double" {
						f_prec = 53
					}
					for j := a_dta_chunk.usedlen - 1; j >= 0; j-- {
						cell := a_dta_chunk.rows[j].cols[n]
						if !cell.Valid {
							lastTable.buf_arr[arr_ind].kind = -1
							b_siz += 4
							arr_ind -= arr_ind_inc
							continue
						}
						f := new(big.Float).SetPrec(f_prec)
						f.SetString(cell.String)
						v := f.Text('f', -1)
						b_siz += len(v)
						lastTable.buf_arr[arr_ind].kind = 0
						lastTable.buf_arr[arr_ind].val = &v
						arr_ind -= arr_ind_inc
					}
				} else {
					for j := a_dta_chunk.usedlen - 1; j >= 0; j-- {
						cell := a_dta_chunk.rows[j].cols[n]
						if !cell.Valid {
							lastTable.buf_arr[arr_ind].kind = -1
							b_siz += 4
							arr_ind -= arr_ind_inc
							continue
						}
						lastTable.buf_arr[arr_ind].kind = 0
						lastTable.buf_arr[arr_ind].val = &cell.String
						b_siz += len(cell.String)
						arr_ind -= arr_ind_inc
					}
				}
				// --------------------------------------------------
			}
			// ----------------------------------------------------------
			var b strings.Builder
			b.Grow(b_siz)
			for n := range lastTable.buf_arr[:last_cell_pos] {
				a_cell := lastTable.buf_arr[n]
				switch a_cell.kind {
				case -1:
					b.WriteString(nullStr)
				case 0:
					b.WriteString(*a_cell.val)
				case 1:
					b.WriteString("'")
					b.WriteString(*a_cell.val)
					b.WriteString("'")
				case 2:
					if dst_driver == "mysql" {
						b.WriteString("_binary '")
						b.WriteString(*a_cell.val)
						b.WriteString("'")
					} else {
						b.WriteString("decode('")
						b.WriteString(*a_cell.val)
						b.WriteString("','hex')")
					}
				case 3:
					b.WriteString("E'")
					b.WriteString(*a_cell.val)
					b.WriteString("'")
				}
			}
			b.WriteString(endStr)
			a_str := b.String()
			if mode_trace {
				log.Printf("%s", a_str)
			}
			if mode_debug {
				if len(a_str) != b_siz {
					log.Printf("[%02d] dataChunkGenerator bad estimation real %d vs esti %d", id, len(a_str), b_siz)
				}
			}
			// ----------------------------------------------------------
			if mode_debug {
				log.Printf("[%02d] dataChunkGenerator table %03d chunk %12d ... sql len is %6d", id, a_dta_chunk.table_id, a_dta_chunk.chunk_id, len(a_str))
			}
			sql2inject <- insertchunk{table_id: lastTable.table_id, chunk_id: a_dta_chunk.chunk_id, sql: &a_str}
			if mode_debug {
				log.Printf("[%02d] dataChunkGenerator table %03d chunk %12d ... ok sql2inject", id, a_dta_chunk.table_id, a_dta_chunk.chunk_id)
			}
			// ----------------------------------------------------------
		}
	}
	// --------------------------------------------------------------------------
	if dumpmode == "csv" {
		// ------------------------------------------------------------------
		var buf_arr []*string
		var last_table_id int = -1
		var tab_meta *MetadataTable
		// ------------------------------------------------------------------
		a_dta_chunk := <-rowvalueschan
		for {
			if a_dta_chunk.table_id == -1 {
				break
			}
			if last_table_id != a_dta_chunk.table_id {
				last_table_id = a_dta_chunk.table_id
				tab_meta = &tableInfos[last_table_id]
				// -------------------------------------------------
				// we use a array of string to generate the final csv
				//
				//   row 1 | col1  | , |  col2 | , | ....   | coln | <return line>
				//   row 2 | col1  | , |  col2 | , | ....   | coln | <return line>
				//   row 3 | col1  | , |  col2 | , | ....   | coln | <return line>
				//   row 4 | col1  | , |  col2 | , | ....   | coln | <return line>
				//
				//    4 rows with n cols => 4*cols*2
				//
				//  echo row need cntCols     for value
				//                cntCols - 1 for sep
				//                1           for line break
				//
				//  total cells => insert_size x ( tab_meta.cntCols * 2 ) cells
				//
				buf_arr = make([]*string, insert_size*2*tab_meta.cntCols)
				coma_str := ","
				eol_str := "\n"
				b_ind := 1
				for r := 0; r < insert_size; r++ {
					for c := 1; c < tab_meta.cntCols; c++ {
						buf_arr[b_ind] = &coma_str
						b_ind += 2
					}
					buf_arr[b_ind] = &eol_str
					b_ind += 2
				}
			}
			// ----------------------------------------------------------
			nullStr := "\\N"
			emptStr := ""
			b_siz := a_dta_chunk.usedlen * tab_meta.cntCols
			// ----------------------------------------------------------
			for n := 0; n < tab_meta.cntCols; n++ {
				b_ind := n * 2
				b_ind_inc := 2 * tab_meta.cntCols
				// -------------------------------------------------
				if tab_meta.columnInfos[n].haveFract {
					for j := 0; j < a_dta_chunk.usedlen; j++ {
						if !a_dta_chunk.rows[j].cols[n].Valid {
							buf_arr[b_ind] = &emptStr
						} else {
							timeSec, timeFract, dotFound := strings.Cut(a_dta_chunk.rows[j].cols[n].String, ".")
							if dotFound {
								timeFract = strings.TrimRight(timeFract, "0")
								if len(timeFract) == 1 {
									timeFract = timeFract + "0"
								}
								a_str := timeSec + "." + timeFract
								buf_arr[b_ind] = &a_str
							} else {
								buf_arr[b_ind] = &a_dta_chunk.rows[j].cols[n].String
							}
							b_siz += len(*buf_arr[b_ind])
						}
						b_ind += b_ind_inc
					}
				} else if tab_meta.columnInfos[n].mustBeQuote && (tab_meta.columnInfos[n].isKindChar || tab_meta.columnInfos[n].isKindBinary) {
					for j := 0; j < a_dta_chunk.usedlen; j++ {
						if !a_dta_chunk.rows[j].cols[n].Valid {
							buf_arr[b_ind] = &nullStr
							b_siz += 2
						} else {
							if needQuoteForCsv(&a_dta_chunk.rows[j].cols[n].String) {
								a_str := "\"" + strings.ReplaceAll(a_dta_chunk.rows[j].cols[n].String, "\"", "\"\"") + "\""
								buf_arr[b_ind] = &a_str
							} else {
								buf_arr[b_ind] = &a_dta_chunk.rows[j].cols[n].String
							}
							b_siz += len(*buf_arr[b_ind])
						}
						b_ind += b_ind_inc
					}
				} else {
					for j := 0; j < a_dta_chunk.usedlen; j++ {
						if !a_dta_chunk.rows[j].cols[n].Valid {
							buf_arr[b_ind] = &emptStr
						} else {
							buf_arr[b_ind] = &a_dta_chunk.rows[j].cols[n].String
							b_siz += len(*buf_arr[b_ind])
						}
						b_ind += b_ind_inc
					}
				}
				// -------------------------------------------------
			}
			var b strings.Builder
			b.Grow(b_siz)
			for n := range buf_arr[:a_dta_chunk.usedlen*2*tab_meta.cntCols] {
				b.WriteString(*buf_arr[n])
			}
			j_str := b.String()
			sql2inject <- insertchunk{table_id: last_table_id, sql: &j_str}
			// ----------------------------------------------------------
			a_dta_chunk = <-rowvalueschan
		}
	}
	// --------------------------------------------------------------------------
	if dumpmode == "nul" {
		// ------------------------------------------------------------------
		a_dta_chunk := <-rowvalueschan
		for {
			if a_dta_chunk.table_id == -1 {
				break
			}
			// ----------------------------------------------------------
			a_dta_chunk = <-rowvalueschan
		}
	}
	// ----------------------------------------------------------------------------------
}

// ------------------------------------------------------------------------------------------
type cachetableFileWriter struct {
	table_id     int
	lastusagecnt int
	und_fh       *os.File
	zst_enc      *zstd.Encoder
}

// ------------------------------------------------------------------------------------------
func tableFileWriter(sql2inject chan insertchunk, id int, tableInfos []MetadataTable, dumpdir string, dumpfiletemplate string, dumpmode string, dumpheader bool, dumpcompress string, z_level int, z_para int, cntBrowser int) {
	if mode_debug {
		if dumpcompress == "zstd" {
			log.Printf("tableFileWriter[%d] start mode %s %s lvl %d conc %d\n", id, dumpmode, dumpcompress, z_level, z_para)
		} else {
			log.Printf("tableFileWriter[%d] start mode %s \n", id, dumpmode)
		}
	}
	// ----------------------------------------------------------------------------------
	file_is_empty := make([]bool, 0)
	file_name := make([]string, 0)
	for _, v := range tableInfos {
		var fname string
		fname = strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(dumpfiletemplate, "%d", v.dbName), "%t", v.tbName), "%p", strconv.Itoa(id)), "%m", "."+dumpmode)
		if dumpcompress == "zstd" {
			fname = strings.ReplaceAll(fname, "%z", ".zst")
		} else {
			fname = strings.ReplaceAll(fname, "%z", "")
		}
		fname = dumpdir + strings.ReplaceAll(fname, "%%", "%")
		if dumpmode != "nul" {
			fh, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				log.Printf("can not openfile %s", fname)
				log.Fatal(err.Error())
			} else {
				fh.Close()
			}
		}
		file_is_empty = append(file_is_empty, true)
		file_name = append(file_name, fname)
	}
	// ----------------------------------------------------------------------------------
	var cntwritechunk int = 0
	var last_hit int = 0
	var cache_hit int = 0
	var cache_miss int = 0

	var tabWrtVars []*cachetableFileWriter
	var lastTable *cachetableFileWriter

	tabWrtVars = make([]*cachetableFileWriter, cntBrowser+1)
	// ----------------------------------------------------------------------------------
	if dumpcompress == "zstd" {
		// --------------------------------------------------------------------------
		for {
			a_insert_sql := <-sql2inject
			if a_insert_sql.sql == nil {
				break
			}
			cntwritechunk++
			// ------------------------------------------------------------------
			if lastTable != nil && lastTable.table_id == a_insert_sql.table_id {
				last_hit++
			} else {
				// ----------------------------------------------------------
				var tab_found int = -1
				var empty_slot int = -1
				var lru_slot int = -1
				var lru_val int = math.MaxInt
				for n := range tabWrtVars {
					if tabWrtVars[n] == nil {
						empty_slot = n
					} else if tabWrtVars[n].table_id == a_insert_sql.table_id {
						tab_found = n
						break
					} else {
						if tabWrtVars[n].lastusagecnt < lru_val {
							lru_val = tabWrtVars[n].lastusagecnt
							lru_slot = n
						}
					}
				}
				if tab_found > -1 {
					cache_hit++
					lastTable = tabWrtVars[tab_found]
					lastTable.lastusagecnt = cntwritechunk
					cache_miss++
				} else {
					if empty_slot == -1 {
						empty_slot = lru_slot
						// ------------------------------------------
						if tabWrtVars[lru_slot].und_fh != nil {
							if tabWrtVars[lru_slot].zst_enc != nil {
								tabWrtVars[lru_slot].zst_enc.Close()
							}
							tabWrtVars[lru_slot].und_fh.Close()
						}
						// ------------------------------------------
					}
					var new_info cachetableFileWriter
					tabWrtVars[empty_slot] = &new_info
					lastTable = &new_info
					// -----------------------------------------
					lastTable.table_id = a_insert_sql.table_id
					// --------------------------------------------------
					fname := file_name[a_insert_sql.table_id]
					var err error
					lastTable.und_fh, err = os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
					if err != nil {
						log.Printf("can not openfile %s", fname)
						log.Fatal(err.Error())
					}
					lastTable.zst_enc, err = zstd.NewWriter(lastTable.und_fh, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(z_level)), zstd.WithEncoderConcurrency(z_para))
					if err != nil {
						log.Printf("can not create zstd.NewWriter")
						log.Fatal(err.Error())
					}
					if file_is_empty[a_insert_sql.table_id] {
						if dumpheader {
							if dumpmode == "csv" {
								ChunkReaderDumpHeader(dumpmode, lastTable.zst_enc, tableInfos[a_insert_sql.table_id].listColsCSV)
							} else {
								ChunkReaderDumpHeader(dumpmode, lastTable.zst_enc, tableInfos[a_insert_sql.table_id].listColsSQL)
							}
						}
						file_is_empty[a_insert_sql.table_id] = false
					}
				}
			}
			// ------------------------------------------------------------------
			if mode_debug {
				log.Printf("[%02d] tableFileWriter table %03d chunk %12d sql len %6d", id, a_insert_sql.table_id, a_insert_sql.chunk_id, len(*a_insert_sql.sql))
			}
			// ------------------------------------------------------------------
			io.WriteString(lastTable.zst_enc, *a_insert_sql.sql)
			// ------------------------------------------------------------------
		}
		// --------------------------------------------------------------------------
		for n := range tabWrtVars {
			if tabWrtVars[n] != nil {
				if tabWrtVars[n].zst_enc != nil {
					tabWrtVars[n].zst_enc.Close()
				}
				tabWrtVars[n].und_fh.Close()
			}
		}
	} else {
		for {
			a_insert_sql := <-sql2inject
			if a_insert_sql.sql == nil {
				break
			}
			cntwritechunk++
			// ------------------------------------------------------------------
			if lastTable != nil && lastTable.table_id == a_insert_sql.table_id {
				last_hit++
			} else {
				// ----------------------------------------------------------
				var tab_found int = -1
				var empty_slot int = -1
				var lru_slot int = -1
				var lru_val int = math.MaxInt
				for n := range tabWrtVars {
					if tabWrtVars[n] == nil {
						empty_slot = n
					} else if tabWrtVars[n].table_id == a_insert_sql.table_id {
						tab_found = n
						break
					} else {
						if tabWrtVars[n].lastusagecnt < lru_val {
							lru_val = tabWrtVars[n].lastusagecnt
							lru_slot = n
						}
					}
				}
				if tab_found > -1 {
					cache_hit++
					lastTable = tabWrtVars[tab_found]
					lastTable.lastusagecnt = cntwritechunk
					cache_miss++
				} else {
					if empty_slot == -1 {
						empty_slot = lru_slot
						// ------------------------------------------
						if tabWrtVars[lru_slot].und_fh != nil {
							tabWrtVars[lru_slot].und_fh.Close()
						}
						// ------------------------------------------
					}
					var new_info cachetableFileWriter
					tabWrtVars[empty_slot] = &new_info
					lastTable = &new_info
					// -----------------------------------------
					lastTable.table_id = a_insert_sql.table_id
					// --------------------------------------------------
					fname := file_name[lastTable.table_id]
					var err error
					lastTable.und_fh, err = os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
					if err != nil {
						log.Printf("can not openfile %s", fname)
						log.Fatal(err.Error())
					}
					if file_is_empty[lastTable.table_id] {
						if dumpheader {
							if dumpmode == "csv" {
								ChunkReaderDumpHeader(dumpmode, lastTable.und_fh, tableInfos[lastTable.table_id].listColsCSV)
							} else {
								ChunkReaderDumpHeader(dumpmode, lastTable.und_fh, tableInfos[lastTable.table_id].listColsSQL)
							}
						}
						file_is_empty[lastTable.table_id] = false
					}
				}
			}
			// ------------------------------------------------------------------
			io.WriteString(lastTable.und_fh, *a_insert_sql.sql)
			// ------------------------------------------------------------------
			if mode_debug {
				log.Printf("[%02d] tableFileWriter table %03d chunk %12d wait next", id, a_insert_sql.table_id, a_insert_sql.chunk_id)
			}
			// ------------------------------------------------------------------
		}
		// --------------------------------------------------------------------------
		for n := range tabWrtVars {
			if tabWrtVars[n] != nil {
				tabWrtVars[n].und_fh.Close()
			}
		}
	}
	// ----------------------------------------------------------------------------------
	if mode_debug {
		log.Printf("tableFileWriter[%d] finish\n", id)
	}
}

// ------------------------------------------------------------------------------------------
func tableCopyWriter(sql2inject chan insertchunk, adbConn *sql.Conn, id int) {
	if mode_debug {
		log.Printf("tableCopyWriter[%d] start\n", id)
	}
	a_insert_sql := <-sql2inject
	for a_insert_sql.sql != nil {
		// --------------------------------------------------------------------------
		_, e_err := adbConn.ExecContext(context.Background(), *a_insert_sql.sql)
		if e_err != nil {
			log.Printf("error with :\n%s", *a_insert_sql.sql)
			log.Fatalf("thread %d , can not insert a chunk\n%s\n", id, e_err.Error())
		}
		// --------------------------------------------------------------------------
		a_insert_sql = <-sql2inject
	}
	if mode_debug {
		log.Printf("tableCopyWriter[%d] finish\n", id)
	}
}

// ------------------------------------------------------------------------------------------

type arrayFlags []string

func (i *arrayFlags) String() string {
	var str_out string
	for key, value := range *i {
		if key != 0 {
			str_out = str_out + " " + value
		} else {
			str_out = value
		}
	}
	return str_out
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

// ------------------------------------------------------------------------------------------
func main() {
	log.SetFlags(log.Ldate | log.Lmicroseconds)
	// ----------------------------------------------------------------------------------
	arg_cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	arg_debug := flag.Bool("debug", false, "debug mode")
	arg_trace := flag.Bool("trace", false, "trace mode")
	arg_loop := flag.Int("loopcnt", 1, "how many times we are going to loop (for debugging)")
	// ----------------------------------------------------------------------------------
	arg_db_driver := flag.String("driver", "mysql", "SQL engine , mysql / postgres")
	arg_db_port := flag.Int("port", 3306, "the database port")
	arg_db_host := flag.String("host", "127.0.0.1", "the database host")
	arg_db_user := flag.String("user", "mysql", "the database connection user")
	arg_db_pasw := flag.String("pwd", "", "the database connection password")
	arg_browser_parr := flag.Int("browser", 4, "number of browsers")
	arg_db_parr := flag.Int("parallel", 10, "number of workers")
	arg_chunk_size := flag.Int("chunksize", 10000, "rows count when reading")
	arg_insert_size := flag.Int("insertsize", 500, "rows count for each insert")
	arg_dumpfile := flag.String("dumpfile", "dump_%d_%t_%p%m%z", "template for dump filename of tables")
	arg_dumpdir := flag.String("dumpdir", "", "directory for dump of tables")
	arg_dumpmode := flag.String("dumpmode", "sql", "format of the dump , csv / sql or cpy (copy between 2 databases instances)")
	arg_dumpheader := flag.Bool("dumpheader", true, "add a header on csv/sql")
	arg_dumpinsert := flag.String("dumpinsert", "full", "specify column names on insert , full /simple ")
	arg_dumpparr := flag.Int("dumpparallel", 5, "number of file writers")
	arg_dumpcompress := flag.String("dumpcompress", "", "which compression format to use , zstd")
	arg_dumpcompress_level := flag.Int("dumpcompresslevel", 1, "which zstd compression level ( 1 , 3 , 6 , 11 ) ")
	arg_dumpcompress_concur := flag.Int("dumpcompressconcur", 4, "which zstd compression concurency ")
	// ------------
	arg_db_name := flag.String("db", "", "the database to connect ( postgres only) ")
	arg_dst_db_name := flag.String("dst-db", "", "the database to connect ( postgres only) ")
	// ------------
	var arg_schemas arrayFlags
	flag.Var(&arg_schemas, "schema", "schema(s) of tables to dump")
	// ------------
	var arg_tables2dump arrayFlags
	flag.Var(&arg_tables2dump, "table", "table to dump")
	// ------------
	var arg_tables2exclude arrayFlags
	flag.Var(&arg_tables2exclude, "exclude-table", "table to exclude")
	// ------------
	arg_guess_pk := flag.Bool("guessprimarykey", false, "guess a primary key in case table does not have one")
	arg_all_tables := flag.Bool("alltables", false, "all tables of the specified database")
	// ------------
	arg_dst_db_driver := flag.String("dst-driver", "mysql", "SQL engine , mysql / postgres")
	arg_dst_db_port := flag.Int("dst-port", 3306, "the database port")
	arg_dst_db_host := flag.String("dst-host", "127.0.0.1", "the database host")
	arg_dst_db_user := flag.String("dst-user", "mysql", "the database connection user")
	arg_dst_db_pasw := flag.String("dst-pwd", "", "the database connection password")
	arg_dst_db_parr := flag.Int("dst-parallel", 20, "number of workers")
	// ------------
	flag.Parse()
	// ------------
	if len(flag.Args()) > 0 {
		log.Printf("extra arguments ( non-recognized) on command line")
		for _, v := range flag.Args() {
			log.Printf(" '%s'    is not recognized \n", v)
		}
		flag.Usage()
		os.Exit(11)
	}
	// ----------------------------------------------------------------------------------
	if arg_tables2dump == nil && !*arg_all_tables {
		log.Printf("no tables specified")
		flag.Usage()
		os.Exit(2)
	}
	if arg_tables2dump != nil && *arg_all_tables {
		log.Printf("can not use -alltables with -table")
		flag.Usage()
		os.Exit(3)
	}
	if arg_schemas == nil {
		log.Printf("no schema specified")
		flag.Usage()
		os.Exit(4)
	}
	if len(arg_schemas) > 1 && arg_tables2dump != nil {
		log.Printf("can not specify multiple schemas with a list of tables")
		flag.Usage()
		os.Exit(5)
	}
	if len(*arg_dumpfile) == 0 && *arg_dumpmode != "cpy" {
		log.Printf("the parameter dumpfile is empty")
		flag.Usage()
		os.Exit(6)
	}
	if len(*arg_dumpmode) != 0 && (*arg_dumpmode != "sql" && *arg_dumpmode != "csv" && *arg_dumpmode != "nul" && *arg_dumpmode != "cpy") {
		log.Printf("invalid value for dumpmode")
		flag.Usage()
		os.Exit(7)
	}
	if len(*arg_dumpcompress) != 0 && (*arg_dumpcompress != "zstd") {
		log.Printf("invalid value for dumpcompress")
		flag.Usage()
		os.Exit(8)
	}
	if *arg_insert_size > *arg_chunk_size {
		log.Printf("invalid values for chunksize ,insertsize\n insertsize (%d) must be less or equal then chunksize (%d)", *arg_insert_size, *arg_chunk_size)
		flag.Usage()
		os.Exit(9)
	}
	if *arg_dumpcompress_level < 1 || *arg_dumpcompress_level > 22 {
		flag.Usage()
		os.Exit(9)
	}
	if len(*arg_dumpcompress) != 0 && (*arg_dumpcompress == "zstd") && len(*arg_dumpmode) != 0 && *arg_dumpmode == "cpy" {
		flag.Usage()
		os.Exit(10)
	}
	if arg_tables2dump != nil && len(arg_tables2exclude) != 0 {
		log.Printf("can not specify table to exclude with a list of tables")
		flag.Usage()
		os.Exit(12)
	}
	if len(*arg_dumpfile) != 0 && len(*arg_dumpdir) != 0 && strings.ContainsRune(*arg_dumpfile, os.PathSeparator) {
		flag.Usage()
		os.Exit(13)
	}
	if len(*arg_db_name) != 0 && (*arg_db_driver == "mysql") {
		flag.Usage()
		os.Exit(16)
	}
	if len(*arg_dst_db_name) != 0 && (*arg_dst_db_driver == "mysql") {
		flag.Usage()
		os.Exit(17)
	}
	if len(*arg_db_name) == 0 && (*arg_db_driver == "postgres") {
		flag.Usage()
		os.Exit(18)
	}
	if len(*arg_dst_db_name) == 0 && (*arg_dst_db_driver == "postgres") {
		flag.Usage()
		os.Exit(19)
	}
	mode_trace = *arg_trace
	if mode_trace {
		mode_debug = true
	} else {
		mode_debug = *arg_debug
	}
	zstd_level := *arg_dumpcompress_level
	zstd_concur := *arg_dumpcompress_concur
	// ----------------------------------------------------------------------------------
	if len(*arg_dumpdir) != 0 && (*arg_dumpdir)[len(*arg_dumpdir)-1] != os.PathSeparator && len(*arg_dumpfile) != 0 && (*arg_dumpfile)[0] != os.PathSeparator {
		new_dir := *arg_dumpdir + string(os.PathSeparator)
		arg_dumpdir = &new_dir
	}
	// ----------------------------------------------------------------------------------
	if len(*arg_dumpdir) != 0 && (*arg_dumpdir)[len(*arg_dumpdir)-1] == os.PathSeparator && len(*arg_dumpfile) != 0 && (*arg_dumpfile)[0] == os.PathSeparator {
		new_dir := (*arg_dumpdir)[0 : len(*arg_dumpdir)-1]
		arg_dumpdir = &new_dir
	}
	template_file := *arg_dumpfile
	for {
		p := strings.IndexRune(template_file, '%')
		if p == -1 {
			break
		}
		if p+1 == len(template_file) {
			flag.Usage()
			os.Exit(14)
		}
		if template_file[p+1] != 'd' && template_file[p+1] != 't' && template_file[p+1] != 'p' && template_file[p+1] != 'm' && template_file[p+1] != 'z' && template_file[p+1] != '%' {
			flag.Usage()
			os.Exit(15)
		}
		template_file = template_file[p+2:]
	}
	// ----------------------------------------------------------------------------------
	var cntBrowser int = *arg_browser_parr
	var cntReader int = *arg_db_parr
	// ----------------------------------------------------------------------------------
	var conSrc []*sql.Conn
	var conDst []*sql.Conn
	var dbSrc *sql.DB
	var dbDst *sql.DB
	if *arg_db_driver == "mysql" {
		dbSrc, conSrc, _, _ = GetaSynchronizedMysqlConnections(*arg_db_host, *arg_db_port, *arg_db_user, *arg_db_pasw, cntReader+cntBrowser, arg_schemas[0])
	}
	if *arg_db_driver == "postgres" {
		dbSrc, conSrc, _, _ = GetaSynchronizedPostgresConnections(*arg_db_host, *arg_db_port, *arg_db_user, *arg_db_pasw, cntReader+cntBrowser, arg_schemas[0])
	}
	if *arg_dumpmode == "cpy" {
		if *arg_dst_db_driver == "mysql" {
			dbDst, conDst, _ = GetDstMysqlConnections(*arg_dst_db_host, *arg_dst_db_port, *arg_dst_db_user, *arg_dst_db_pasw, *arg_dst_db_parr, arg_schemas[0])
		}
		if *arg_dst_db_driver == "postgres" {
			dbDst, conDst, _ = GetDstPostgresConnections(*arg_dst_db_host, *arg_dst_db_port, *arg_dst_db_user, *arg_dst_db_pasw, *arg_dst_db_parr, *arg_dst_db_name)
		}
	}
	// ----------------------------------------------------------------------------------
	var tables2dump []aTable
	if arg_tables2dump == nil {
		tables2dump = GetListTables(conSrc[0], arg_schemas, arg_tables2exclude)
	} else {
		for _, t := range arg_tables2dump {
			tables2dump = append(tables2dump, aTable{dbName: arg_schemas[0], tbName: t})
		}
	}
	if mode_debug {
		log.Print(tables2dump)
	}
	r, _ := GetMetadataInfo4Tables(conSrc[0], tables2dump, *arg_guess_pk, *arg_dumpmode, *arg_dumpinsert, *arg_db_driver, *arg_dst_db_driver)
	if mode_debug {
		log.Printf("tables infos  => %s", r)
	}
	if *arg_dumpmode == "cpy" {
		CheckTablesOnDestination(*arg_db_driver, *arg_dst_db_driver, conDst[0], r)
	}
	// ----------------------------------------------------------------------------------
	if *arg_cpuprofile != "" {
		f, err := os.Create(*arg_cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	// ----------------------------------------------------------------------------------
	//
	// browser   : scan tables on db to compute border of each chunk  and pass to a reader
	// reader    : read chunks quickly from the db , split them into blocks , pass them to a generator
	// generator : generate sql/csv string from blocks , and pass them to a writer
	// writer    : receive a sql/csv string and write info a file or inject into a db
	//
	var wg_brw sync.WaitGroup
	var wg_red sync.WaitGroup
	var wg_gen sync.WaitGroup
	var wg_wrt sync.WaitGroup
	// ----------------------------------------------------------------------------------
	tables_to_browse := make(chan int, len(tables2dump)**arg_loop+cntBrowser)
	pk_chunks_to_read := make(chan tablechunk, *arg_db_parr*200)
	sql_to_write := make(chan insertchunk, *arg_db_parr*400)
	sql_generator := make(chan datachunk, *arg_db_parr*1000)
	if mode_debug {
		pk_chunks_to_read = make(chan tablechunk, 5)
		sql_to_write = make(chan insertchunk, 5)
		sql_generator = make(chan datachunk, 5)
	}
	if mode_trace {
		pk_chunks_to_read = make(chan tablechunk, 1)
		sql_to_write = make(chan insertchunk, 1)
		sql_generator = make(chan datachunk, 1)
	}
	// ------------
	for l := 0; l < *arg_loop; l++ {
		for t := 0; t < len(tables2dump); t++ {
			tables_to_browse <- t
		}
	}
	for b := 0; b < cntBrowser; b++ {
		tables_to_browse <- -1
	}
	// ------------
	for j := 0; j < cntBrowser; j++ {
		wg_brw.Add(1)
		go func(adbConn *sql.Conn, id int) {
			defer wg_brw.Done()
			tableChunkBrowser(adbConn, id, tables_to_browse, r, pk_chunks_to_read, int64(*arg_chunk_size))
		}(conSrc[j], j)
	}
	// ------------
	for j := 0; j < cntReader; j++ {
		time.Sleep(10 * time.Millisecond)
		wg_red.Add(1)
		go func(adbConn *sql.Conn, id int) {
			defer wg_red.Done()
			tableChunkReader(pk_chunks_to_read, sql_generator, adbConn, r, id, *arg_insert_size, cntBrowser)
		}(conSrc[j+cntBrowser], j)
	}
	// ------------
	gener_cnt := *arg_db_parr * 2
	if mode_debug {
		gener_cnt = 2
	}
	for j := 0; j < gener_cnt; j++ {
		wg_gen.Add(1)
		go func(id int) {
			defer wg_gen.Done()
			dataChunkGenerator(sql_generator, id, r, *arg_dumpmode, sql_to_write, *arg_insert_size, *arg_dst_db_driver, cntBrowser)
		}(j)
	}
	// ------------
	writer_cnt := 0
	if len(conDst) > 0 {
		writer_cnt = len(conDst)
		for j := 0; j < writer_cnt; j++ {
			wg_wrt.Add(1)
			go func(adbConn *sql.Conn, id int) {
				defer wg_wrt.Done()
				tableCopyWriter(sql_to_write, adbConn, id+len(conSrc))
			}(conDst[j], j)
		}
	} else {
		writer_cnt = *arg_dumpparr
		for j := 0; j < *arg_dumpparr; j++ {
			wg_wrt.Add(1)
			go func(id int) {
				defer wg_wrt.Done()
				tableFileWriter(sql_to_write, id, r, *arg_dumpdir, *arg_dumpfile, *arg_dumpmode, *arg_dumpheader, *arg_dumpcompress, zstd_level, zstd_concur, cntBrowser)
			}(j)
		}
	}
	// ------------
	wg_brw.Wait()
	log.Print("we are done with browser")
	// ------------
	for j := 0; j < cntReader; j++ {
		var a_chunk tablechunk
		a_chunk.table_id = -1
		a_chunk.chunk_id = -1
		a_chunk.begin_val = make([]string, 0)
		a_chunk.end_val = make([]string, 0)
		a_chunk.begin_equal_end = false
		a_chunk.is_done = true
		pk_chunks_to_read <- a_chunk
	}
	if mode_debug {
		log.Printf("we added %d empty chunk to flush readers", cntReader)
	}
	wg_red.Wait()
	log.Print("we are done with reader")
	// ------------
	for j := 0; j < gener_cnt; j++ {
		sql_generator <- datachunk{table_id: -1, rows: nil}
	}
	if mode_debug {
		log.Printf("we added %d nil pointer to flush generators", gener_cnt)
	}
	wg_gen.Wait()
	log.Print("we are done with generators")
	// ------------
	for j := 0; j < writer_cnt; j++ {
		sql_to_write <- insertchunk{table_id: 0, sql: nil}
	}
	if mode_debug {
		log.Printf("we added %d nil pointer to flush writers", writer_cnt)
	}
	wg_wrt.Wait()
	log.Print("we are done with writers")
	// ----------------------------------------------------------------------------------
	dbSrc.Close()
	if len(conDst) > 0 {
		dbDst.Close()
	}
}

// ------------------------------------------------------------------------------------------
