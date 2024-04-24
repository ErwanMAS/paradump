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
	"os"
	"reflect"
	"regexp"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
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
const (
	Action_Undefined = iota
	Action_Read
	Action_Write
	Action_NoOp
)

const (
	Phase_Undefined = iota
	Phase_Browser
	Phase_SrcReader
	Phase_DstReader
	Phase_DstWriter
)

type action2Stat struct {
	Action int
	Phase  int
	TabId  int
	Cnt    int
}

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
func GetaSynchronizedMsSqlConnections(DbHost string, DbPort int, DbUsername string, DbUserPassword string, TargetCount int, ConDatabase string) (*sql.DB, []*sql.Conn, StatMysqlSession, error) {
	// https://github.com/microsoft/go-mssqldb#parameters
	// https://github.com/microsoft/go-mssqldb#the-connection-string-can-be-specified-in-one-of-three-formats
	// sqlserver://username:password@host:port?param1=value&param2=value
	db, err := sql.Open("sqlserver", fmt.Sprintf("postgres://%s@%s:%s:%d/%s?sslmode=disable", DbUsername, DbUserPassword, DbHost, DbPort, ConDatabase))
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
func GetDstMsSqlConnections(DbHost string, DbPort int, DbUsername string, DbUserPassword string, TargetCount int, ConDatabase string) (*sql.DB, []*sql.Conn, error) {
	// https://github.com/microsoft/go-mssqldb#the-connection-string-can-be-specified-in-one-of-three-formats
	// sqlserver://username:password@host:port?param1=value&param2=value
	db, err := sql.Open("sqlserver", fmt.Sprintf("sqlserver://%s:%s@%s:%d/?encrypt=disable&REPLICATION=TRUE&DATABASE=%s", DbUsername, DbUserPassword, DbHost, DbPort, ConDatabase))
	if err != nil {
		log.Print("can not create a MsSql object")
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
			log.Print("can not open a MsSql connection")
			log.Fatal(err.Error())
		}
		db_conns[i] = first_conn
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
	isKindDate   bool
}

type indexInfo struct {
	idxName     string
	cardinality int64
	columns     []columnInfo
}

type aTable struct {
	dbName    string
	tbName    string
	dstDbName string
}

type MetadataTable struct {
	dbName                           string
	dstDbName                        string
	tbName                           string
	isEmpty                          bool
	cntRows                          int64
	sizeBytes                        int64
	storageEng                       string
	cntCols                          int
	cntPkCols                        int
	columnInfos                      []columnInfo
	dstColumnInfos                   []columnInfo
	primaryKey                       []string
	primaryKeyEnum                   []string
	Indexes                          []indexInfo
	fakePrimaryKey                   bool
	withTrigger                      bool
	onError                          int
	fullName                         string
	listColsSQL                      string
	listColsPkSQL                    string
	listColsPkFetchSQL               string
	listColsPkOrderSQL               string
	listColsPkOrderDescSQL           string
	listColsCSV                      string
	query_for_browser_first          string
	query_for_browser_next           string
	param_indices_browser_next_qry   []int
	query_for_reader_interval        string
	query_for_dst_reader_interval    string
	param_indices_interval_lo_qry    []int
	param_indices_interval_up_qry    []int
	query_for_reader_upper_bound     string
	query_for_dst_reader_upper_bound string
	param_indices_upper_bound_qry    []int
	query_for_reader_lower_bound     string
	query_for_dst_reader_lower_bound string
	param_indices_lower_bound_qry    []int
	query_for_insert                 []string
	query_for_insert_size            int
	query_for_update                 []string
	query_for_update_size            int
	query_for_delete                 []string
	query_for_delete_size            int
	insert_size                      int
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
		a_col.isKindDate = a_col.colType == "date" || a_col.colType == "datetime" || a_col.colType == "time" || a_col.colType == "timestamp"
		a_col.mustBeQuote = a_col.isKindChar || a_col.isKindBinary || a_col.isKindDate
		a_col.isKindFloat = (a_col.colType == "float" || a_col.colType == "double")
		a_col.haveFract = (a_col.colType == "datetime" || a_col.colType == "timestamp" || a_col.colType == "time") && (a_col.dtPrec > 0)
		re := regexp.MustCompile(`int(\([0-9]*\))`)
		a_col.colSqlType = re.ReplaceAllString(a_col.colSqlType, "int")
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
func GetMsSqlBasicMetadataInfo(adbConn *sql.Conn, dbName string, tableName string) (MetadataTable, bool) {
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

	q_rows, q_err := adbConn.QueryContext(ctx, "SELECT      SUM(a.total_pages) * 8 AS TotalSpaceKB, max(p.rows), 'UNKNOW',is_t.TABLE_TYPE "+
		"            from information_schema.tables is_t "+
		"            join sys.tables t on is_t.TABLE_NAME = t.name "+
		"		INNER JOIN sys.indexes i ON t.object_id = i.object_id "+
		"		INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id "+
		"		INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id "+
		"		LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id and s.name=is_t.table_schema "+
		"		WHERE "+
		"		      is_t.table_schema= @p1 and is_t.table_name=@p2 "+
		"		GROUP BY is_t.TABLE_TYPE , is_t.table_schema , is_t.table_name  ", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query information_schema.tables for %s.%s\n%s", dbName, tableName, q_err.Error())
	}
	typeTable := "DO_NOT_EXIST"
	for q_rows.Next() {
		err := q_rows.Scan(&result.sizeBytes, &result.cntRows, &result.storageEng, &typeTable)
		if err != nil {
			log.Printf("can not scan for query information_schema.tables for %s.%s", dbName, tableName)
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

	q_rows, q_err = adbConn.QueryContext(ctx, "select COLUMN_NAME , DATA_TYPE,IS_NULLABLE,COALESCE(DATETIME_PRECISION,-9999),COALESCE(numeric_precision,-9999) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = @p1 AND table_name = @p2 order by ORDINAL_POSITION ", dbName, tableName)
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
		" SELECT column_name as PRIMARYKEYCOLUMN FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY'  AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND KU.table_name=@P2 AND KU.TABLE_SCHEMA=@P1 ",
		dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query INFORMATION_SCHEMA.TABLE_CONSTRAINTS to get primary key info for %s.%s\n%s", dbName, tableName, q_err.Error())
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

		q_rows, q_err = adbConn.QueryContext(ctx, fmt.Sprintf("select top 1 1 from %s ", result.fullName))
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
		q_rows, q_err = adbConn.QueryContext(ctx, "SELECT count(*) FROM sys.triggers tr INNER JOIN sys.tables t INNER JOIn sys.schemas s ON t.schema_id = s.schema_id ON t.object_id = tr.parent_id WHERE  s.name = @P1 and t.name=@P2", dbName, tableName)
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

// -----------------------------------------------------------------------------------------
func GetTableMetadataInfo(adbConn *sql.Conn, dbName string, tableName string, guessPk bool, srcdriver string, dstdriver string, dstDbName string) (MetadataTable, bool) {
	result, _ := GetMysqlBasicMetadataInfo(adbConn, dbName, tableName)
	result.dstDbName = dstDbName
	// ---------------------------------------------------------------------------------
	enum_in_pk := (dstdriver != "mssql")
	filter_collect_index := " and INDEX_NAME != 'PRIMARY' "
	//
	enumPkCols := make([]string, 0)
	if len(result.primaryKey) > 0 {
		// -------------------------------------------------------------------------
		for _, v := range result.columnInfos {
			if v.colType == "enum" {
				for _, pk := range result.primaryKey {
					if pk == v.colName {
						enumPkCols = append(enumPkCols, pk)
					}
				}
			}
		}
		result.primaryKeyEnum = enumPkCols
		// -------------------------------------------------------------------------
		if len(enumPkCols) > 0 && (!enum_in_pk) {
			result.primaryKey = nil
			result.primaryKeyEnum = nil
			enumPkCols = nil
			filter_collect_index = ""
		}
	}
	// ---------------------------------------------------------------------------------
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	qry_collect_index := "select COLUMN_NAME,coalesce(CARDINALITY,0),INDEX_NAME  from INFORMATION_SCHEMA.STATISTICS WHERE  table_schema = ? and table_name = ? " + filter_collect_index + " order by INDEX_NAME,SEQ_IN_INDEX"
	q_rows, q_err := adbConn.QueryContext(ctx, qry_collect_index, dbName, tableName)
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
				log.Printf("t.cntRows        : %d", result.cntRows)
				log.Printf("t.sizeBytes      : %d", result.sizeBytes)
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
				log.Printf("t.fakePrimaryKey : %t", result.fakePrimaryKey)
				log.Printf("t.onError        : %d", result.onError)
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
				ix_have_enum := false
				for _, colinfo := range ix.columns {
					ix_have_enum = ix_have_enum || colinfo.colType == "enum"
				}
				if !ix_have_null && ix.cardinality > max_cardinality && ((!ix_have_enum) || (ix_have_enum && enum_in_pk)) {
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
	if mode_debug {
		if len(enumPkCols) > 0 {
			log.Printf(" for table %s we have enum in pk for column %s", result.fullName, enumPkCols)
		}
	}
	// ---------------------------
	sql_cond_lower_pk, qry_indices_lo_bound := generatePredicat(result.primaryKey, true, enumPkCols, "``", "?", 0)
	sql_cond_upper_pk, qry_indices_up_bound := generatePredicat(result.primaryKey, false, enumPkCols, "``", "?", 0)
	// ---------------------------
	result.listColsSQL = generateListCols4Sql(result.columnInfos, true)
	result.listColsPkSQL = generateListPkCols4Sql(result.primaryKey, "")
	result.listColsPkFetchSQL = generateListPkColsFetch4Sql(result.primaryKey, enumPkCols)
	result.listColsPkOrderSQL = generateListPkCols4Sql(result.primaryKey, "")
	result.listColsPkOrderDescSQL = generateListPkCols4Sql(result.primaryKey, "desc")
	result.listColsCSV = generateListCols4Csv(result.columnInfos)
	// ---------------------------
	result.query_for_browser_first = fmt.Sprintf("select %s from %s order by %s limit 1 ", result.listColsPkFetchSQL, result.fullName, result.listColsPkOrderSQL)
	result.query_for_browser_next = fmt.Sprintf("select %s,cast(@cnt as unsigned integer ) as _cnt_pkey from ( select %s,@cnt:=@cnt+1 from %s , ( select @cnt := 0 ) c where %s order by %s limit %%d ) e order by %s limit 1 ",
		result.listColsPkFetchSQL, result.listColsPkSQL, result.fullName, sql_cond_lower_pk, result.listColsPkOrderSQL, result.listColsPkOrderDescSQL)
	result.param_indices_browser_next_qry = qry_indices_lo_bound
	// ---------------------------
	result.query_for_reader_interval = fmt.Sprintf("/* parasync */ select %s from %s where ( %s ) and ( %s) ", result.listColsSQL, result.fullName, sql_cond_lower_pk, sql_cond_upper_pk)
	result.query_for_dst_reader_interval = fmt.Sprintf("/* parasync */ select %s from %s.%s where ( %s ) and ( %s) ", result.listColsSQL, result.dstDbName, result.tbName, sql_cond_lower_pk, sql_cond_upper_pk)
	result.param_indices_interval_lo_qry = qry_indices_lo_bound
	result.param_indices_interval_up_qry = qry_indices_up_bound
	// ---------------------------
	result.query_for_reader_upper_bound = fmt.Sprintf("/* parasync */ select %s from %s where ( %s )           ", result.listColsSQL, result.fullName, sql_cond_upper_pk)
	result.query_for_dst_reader_upper_bound = fmt.Sprintf("/* parasync */ select %s from %s.%s where ( %s )           ", result.listColsSQL, result.dstDbName, result.tbName, sql_cond_upper_pk)
	// ---------------------------
	result.query_for_reader_lower_bound = fmt.Sprintf("/* parasync */ select %s from %s where ( %s )           ", result.listColsSQL, result.fullName, sql_cond_lower_pk)
	result.query_for_dst_reader_lower_bound = fmt.Sprintf("/* parasync */ select %s from %s.%s where ( %s )           ", result.listColsSQL, result.dstDbName, result.tbName, sql_cond_lower_pk)
	// ---------------------------
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
func GetMetadataInfo4Tables(adbConn *sql.Conn, tableNames []aTable, guessPk bool, srcdriver string, dstdriver string) ([]MetadataTable, bool) {
	var result []MetadataTable
	for j := 0; j < len(tableNames); j++ {
		info, _ := GetTableMetadataInfo(adbConn, tableNames[j].dbName, tableNames[j].tbName, guessPk, srcdriver, dstdriver, tableNames[j].dstDbName)
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
func CheckTableOnDestination(driver string, adbConn *sql.Conn, a_table *MetadataTable) (string, bool) {
	var dstinfo MetadataTable
	if driver == "mysql" {
		dstinfo, _ = GetMysqlBasicMetadataInfo(adbConn, a_table.dstDbName, a_table.tbName)
	} else {
		if driver == "mssql" {
			dstinfo, _ = GetMsSqlBasicMetadataInfo(adbConn, a_table.dstDbName, a_table.tbName)
		} else {
			dstinfo, _ = GetPostgresBasicMetadataInfo(adbConn, a_table.dstDbName, a_table.tbName)
		}
	}
	a_table.dstColumnInfos = dstinfo.columnInfos
	if mode_debug {
		log.Printf("tab %s.%s => %s.%s size cols src %2d dst %2d", a_table.dbName, a_table.tbName, a_table.dstDbName, a_table.tbName, len(a_table.columnInfos), len(a_table.dstColumnInfos))
	}
	res := reflect.DeepEqual(a_table.columnInfos, dstinfo.columnInfos)
	err_msg := ""
	if !res {
		err_msg = fmt.Sprintf("columns definitions are not identical for %s and %s", a_table.fullName, dstinfo.fullName)
		if mode_debug {
			log.Printf("src:\n%s\n", a_table.columnInfos)
			log.Printf("dst:\n%s\n", dstinfo.columnInfos)
		}
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
	return err_msg, err_msg != ""
}

// ------------------------------------------------------------------------------------------
func PopulateDstSchema(lst_tab *[]aTable, a_schema *string, a_dst_schema *string) {
	for j := 0; j < len(*lst_tab); j++ {
		if (*lst_tab)[j].dbName == *a_schema {
			(*lst_tab)[j].dstDbName = *a_dst_schema
		}
	}
}

// ------------------------------------------------------------------------------------------
func PopulateDstQueries(dstdriver string, adbConn *sql.Conn, a_table *MetadataTable) {
	if dstdriver == "postgres" {
		dst_sql_cond_lower_pk, a := generatePredicat(a_table.primaryKey, true, nil, "  ", "$%d", 1)
		dst_sql_cond_upper_pk, _ := generatePredicat(a_table.primaryKey, false, nil, "  ", "$%d", 1+len(a))
		dst_sql_cond_bound_pk, _ := generatePredicat(a_table.primaryKey, false, nil, "  ", "$%d", 1)
		dst_listsCols_Sql := generateListCols4Sql(a_table.columnInfos, false)
		// -------------------
		a_table.query_for_dst_reader_interval = fmt.Sprintf("/* parasync */ select %s from %s.%s where ( %s ) and ( %s) ", dst_listsCols_Sql, a_table.dstDbName, a_table.tbName, dst_sql_cond_lower_pk, dst_sql_cond_upper_pk)
		a_table.query_for_dst_reader_lower_bound = fmt.Sprintf("/* parasync */ select %s from %s.%s where ( %s )           ", dst_listsCols_Sql, a_table.dstDbName, a_table.tbName, dst_sql_cond_lower_pk)
		a_table.query_for_dst_reader_upper_bound = fmt.Sprintf("/* parasync */ select %s from %s.%s where ( %s )           ", dst_listsCols_Sql, a_table.dstDbName, a_table.tbName, dst_sql_cond_bound_pk)
		// -------------------
		query_for_insert := fmt.Sprintf("INSERT INTO %s.%s(%s) VALUES ( #", a_table.dstDbName, a_table.tbName, generateListCols4Sql(a_table.columnInfos, false))
		for range a_table.columnInfos[1:] {
			query_for_insert = query_for_insert + " , # "
		}
		query_for_insert = query_for_insert + " ) "
		a_table.query_for_insert = strings.Split(query_for_insert, "#")
		query_for_update := fmt.Sprintf("UPDATE %s.%s set %s = # ", a_table.dstDbName, a_table.tbName, a_table.columnInfos[0].colName)
		for _, v := range a_table.columnInfos[1:] {
			query_for_update = query_for_update + fmt.Sprintf(", %s = # ", v.colName)
		}
		query_for_update = query_for_update + fmt.Sprintf(" where %s # ", a_table.columnInfos[0].colName)
		for _, v := range a_table.columnInfos[1:] {
			query_for_update = query_for_update + fmt.Sprintf("and %s # ", v.colName)
		}
		a_table.query_for_update = strings.Split(query_for_update, "#")
		query_for_delete := fmt.Sprintf("DELETE FROM %s.%s ", a_table.dstDbName, a_table.tbName)
		query_for_delete = query_for_delete + fmt.Sprintf(" where %s # ", a_table.columnInfos[0].colName)
		for _, v := range a_table.columnInfos[1:] {
			query_for_delete = query_for_delete + fmt.Sprintf("and %s # ", v.colName)
		}
		a_table.query_for_delete = strings.Split(query_for_delete, "#")
	} else {
		if dstdriver == "mssql" {
			dst_sql_cond_lower_pk, a := generatePredicat(a_table.primaryKey, true, nil, "  ", "@p%d", 1)
			dst_sql_cond_upper_pk, _ := generatePredicat(a_table.primaryKey, false, nil, "  ", "@p%d", 1+len(a))
			dst_sql_cond_bound_pk, _ := generatePredicat(a_table.primaryKey, false, nil, "  ", "@p%d", 1)
			dst_listsCols_Sql := generateListCols4Sql(a_table.columnInfos, false)
			// -------------------
			a_table.query_for_dst_reader_interval = fmt.Sprintf("/* parasync */ select %s from %s.%s where ( %s ) and ( %s) ", dst_listsCols_Sql, a_table.dstDbName, a_table.tbName, dst_sql_cond_lower_pk, dst_sql_cond_upper_pk)
			a_table.query_for_dst_reader_lower_bound = fmt.Sprintf("/* parasync */ select %s from %s.%s where ( %s )           ", dst_listsCols_Sql, a_table.dstDbName, a_table.tbName, dst_sql_cond_lower_pk)
			a_table.query_for_dst_reader_upper_bound = fmt.Sprintf("/* parasync */ select %s from %s.%s where ( %s )           ", dst_listsCols_Sql, a_table.dstDbName, a_table.tbName, dst_sql_cond_bound_pk)
			// -------------------
			query_for_insert := fmt.Sprintf("INSERT INTO %s.%s(%s) VALUES ( #", a_table.dstDbName, a_table.tbName, generateListCols4Sql(a_table.columnInfos, false))
			for range a_table.columnInfos[1:] {
				query_for_insert = query_for_insert + " , # "
			}
			query_for_insert = query_for_insert + ")"
			a_table.query_for_insert = strings.Split(query_for_insert, "#")
			query_for_update := fmt.Sprintf("UPDATE %s.%s set %s = # ", a_table.dstDbName, a_table.tbName, a_table.columnInfos[0].colName)
			for _, v := range a_table.columnInfos[1:] {
				query_for_update = query_for_update + fmt.Sprintf(", %s = # ", v.colName)
			}
			if a_table.dstColumnInfos[0].colType == "text" {
				query_for_update = query_for_update + fmt.Sprintf(" where cast( %s as nvarchar(MAX)) # ", a_table.columnInfos[0].colName)
			} else {
				query_for_update = query_for_update + fmt.Sprintf(" where %s # ", a_table.columnInfos[0].colName)
			}
			for i, v := range a_table.columnInfos[1:] {
				if a_table.dstColumnInfos[i+1].colType == "text" {
					query_for_update = query_for_update + fmt.Sprintf("and cast( %s as nvarchar(MAX)) # ", v.colName)
				} else {
					query_for_update = query_for_update + fmt.Sprintf("and %s # ", v.colName)
				}
			}
			a_table.query_for_update = strings.Split(query_for_update, "#")
			query_for_delete := fmt.Sprintf("DELETE FROM %s.%s ", a_table.dstDbName, a_table.tbName)
			query_for_delete = query_for_delete + fmt.Sprintf(" where %s # ", a_table.columnInfos[0].colName)
			for _, v := range a_table.columnInfos[1:] {
				query_for_delete = query_for_delete + fmt.Sprintf("and %s # ", v.colName)
			}
			a_table.query_for_delete = strings.Split(query_for_delete, "#")
		} else {
			query_for_insert := fmt.Sprintf("INSERT INTO %s.%s(%s) VALUES ( #", a_table.dstDbName, a_table.tbName, a_table.listColsSQL)
			for range a_table.columnInfos[1:] {
				query_for_insert = query_for_insert + " , # "
			}
			query_for_insert = query_for_insert + ")"
			a_table.query_for_insert = strings.Split(query_for_insert, "#")
			query_for_update := fmt.Sprintf("UPDATE %s.%s set %s = # ", a_table.dstDbName, a_table.tbName, a_table.columnInfos[0].colName)
			for _, v := range a_table.columnInfos[1:] {
				query_for_update = query_for_update + fmt.Sprintf(", %s = # ", v.colName)
			}
			query_for_update = query_for_update + fmt.Sprintf(" where %s # ", a_table.columnInfos[0].colName)
			for _, v := range a_table.columnInfos[1:] {
				query_for_update = query_for_update + fmt.Sprintf("and %s # ", v.colName)
			}
			a_table.query_for_update = strings.Split(query_for_update, "#")
			query_for_delete := fmt.Sprintf("DELETE FROM %s.%s ", a_table.dstDbName, a_table.tbName)
			query_for_delete = query_for_delete + fmt.Sprintf(" WHERE %s # ", a_table.columnInfos[0].colName)
			for _, v := range a_table.columnInfos[1:] {
				query_for_delete = query_for_delete + fmt.Sprintf("and %s # ", v.colName)
			}
			a_table.query_for_delete = strings.Split(query_for_delete, "#")
		}
	}
	for _, s := range a_table.query_for_insert {
		a_table.query_for_insert_size = a_table.query_for_insert_size + len(s)
	}
	for _, s := range a_table.query_for_update {
		a_table.query_for_update_size = a_table.query_for_update_size + len(s)
	}
	for _, s := range a_table.query_for_delete {
		a_table.query_for_delete_size = a_table.query_for_delete_size + len(s)
	}
}

// ------------------------------------------------------------------------------------------
//
// will fetch DDL informations on destination database , and compare table definitions
func CheckTablesOnDestination(srcdriver string, dstdriver string, adbConn *sql.Conn, infTables *[]MetadataTable) {
	cnterr := 0
	for n := range *infTables {
		msg, err := CheckTableOnDestination(dstdriver, adbConn, &(*infTables)[n])
		if err {
			log.Printf("issue with table %s.%s on destination", (*infTables)[n].dbName, (*infTables)[n].tbName)
			log.Printf("%s", msg)
			cnterr++
		}
		if mode_debug {
			log.Printf("tab %s.%s => %s.%s size cols src %2d dst %2d", (*infTables)[n].dbName, (*infTables)[n].tbName, (*infTables)[n].dstDbName, (*infTables)[n].tbName, len((*infTables)[n].columnInfos), len((*infTables)[n].dstColumnInfos))
		}
		PopulateDstQueries(dstdriver, adbConn, &(*infTables)[n])
	}
	if srcdriver != dstdriver {
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
	begin_val_mysql []string
	end_val_mysql   []string
	begin_val_oth   []string
	end_val_oth     []string
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
	tchunk *tablechunk
	rows   []*rowchunk
}

type datachunkpair struct {
	c_src *datachunk
	c_dst *datachunk
}

// ------------------------------------------------------------------------------------------
const (
	DML_Undefined = iota
	DML_Insert
	DML_Update
	DML_Delete
)

// ------------------------------------------------------------------------------------------
type datarow struct {
	table_id int
	dmltype  int
	valdta   *rowchunk
	whrdta   *rowchunk
}

// ------------------------------------------------------------------------------------------
type insertchunk struct {
	table_id int
	dmltype  int
	cntrows  int
	sql      *string
	params   *[]any
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
func generatePredicat(pkeyCols []string, lowerbound bool, enumCols []string, tablequote string, sql_placeholder string, sql_placeholder_start_pos int) (string, []int) {
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
		if len(sql_placeholder) > 2 {
			place_holder = fmt.Sprintf(sql_placeholder, len(sql_vals_indices)+sql_placeholder_start_pos)
		}
		if col_is_enum {
			place_holder = "cast(? as unsigned integer)"
		}
		if ncolpkey == i {
			sql_pred = sql_pred + fmt.Sprintf(" ( %c%s%c %s %s ) ", tablequote[0], pkeyCols[i], tablequote[1], op_1, place_holder)
		} else {
			sql_pred = sql_pred + fmt.Sprintf(" ( %c%s%c %s %s ) ", tablequote[0], pkeyCols[i], tablequote[1], op_o, place_holder)
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
					if len(sql_placeholder) > 2 {
						place_holder = fmt.Sprintf(sql_placeholder, len(sql_vals_indices)+sql_placeholder_start_pos)
					}
					if col_is_enum {
						place_holder = "cast(? as unsigned integer)"
					}

					sql_pred = sql_pred + fmt.Sprintf(" and ( %c%s%c = %s ) ", tablequote[0], pkeyCols[j], tablequote[1], place_holder)
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
func generateEqualityPredicat(pkeyCols []string, enumCols []string, tablequote string, sql_placeholder string) (string, []int) {
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
		if len(sql_placeholder) > 2 {
			place_holder = fmt.Sprintf(sql_placeholder, 1+len(sql_vals_indices))
		}
		if col_is_enum {
			place_holder = " cast( ? as unsigned integer ) "
		}
		sql_pred = sql_pred + fmt.Sprintf(" ( %c%s%c = %s ) ", tablequote[0], pkeyCols[i], tablequote[1], place_holder)
		sql_vals_indices = append(sql_vals_indices, i)
	}
	sql_pred = sql_pred + " ) "
	return sql_pred, sql_vals_indices
}

// ------------------------------------------------------------------------------------------
func pk_extract_values(a_pk_row []string, a_table_inf MetadataTable, enum_cast bool) []string {
	res_val := make([]string, a_table_inf.cntPkCols)
	j := 0
	for i := range a_table_inf.primaryKey {
		col_is_enum := false
		for e := range a_table_inf.primaryKeyEnum {
			if a_table_inf.primaryKeyEnum[e] == a_table_inf.primaryKey[i] {
				col_is_enum = true
				break
			}
		}
		if col_is_enum {
			if enum_cast {
				res_val[i] = a_pk_row[j+1]
			} else {
				res_val[i] = a_pk_row[j]
			}
			j++
		} else {
			res_val[i] = a_pk_row[j]
		}
		j++
	}
	return res_val
}

// ------------------------------------------------------------------------------------------
func tableChunkBrowser(adbConn *sql.Conn, id int, tableidstoscan chan int, tableInfos []MetadataTable, chunk2read chan tablechunk, sizeofchunk_init int, stats2push chan action2Stat) {
	if mode_debug {
		log.Printf("tableChunkBrowser  [%02d]: start\n", id)
	}
	var sizeofchunk int
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
		resultset_pk_size := tableInfos[j].cntPkCols + len(tableInfos[j].primaryKeyEnum)
		if mode_debug {
			log.Printf("tableChunkBrowser  [%02d]: table %s size pk %d query :  %s \n", id, tableInfos[j].fullName, tableInfos[j].cntPkCols, tableInfos[j].query_for_browser_first)
		}
		ctx, _ = context.WithTimeout(context.Background(), 16*time.Second)
		q_rows, q_err := adbConn.QueryContext(ctx, tableInfos[j].query_for_browser_first)
		if q_err != nil {
			log.Fatalf("can not query table %s to get first pk aka ( %s )\n%s", tableInfos[j].fullName, tableInfos[j].listColsPkSQL, q_err.Error())
		}
		a_sql_row := make([]*sql.NullString, resultset_pk_size)
		var pk_cnt int
		ptrs := make([]any, resultset_pk_size)
		var ptrs_nextpk []any
		ptrs_nextpk = make([]any, resultset_pk_size+1)
		for i := range a_sql_row {
			ptrs[i] = &a_sql_row[i]
			ptrs_nextpk[i] = &a_sql_row[i]
		}
		ptrs_nextpk[resultset_pk_size] = &pk_cnt
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
		start_pk_row := make([]string, resultset_pk_size)
		for n, value := range a_sql_row {
			start_pk_row[n] = value.String
		}
		log.Printf("tableChunkBrowser  [%02d]: table["+format_cnt_table+"] %s first pk ( %s ) - start scan pk %s\n", id, j, tableInfos[j].fullName, tableInfos[j].listColsPkSQL, pk_extract_values(start_pk_row, tableInfos[j], true))
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
		chunk_id++
		chunk2read <- tablechunk{table_id: j, chunk_id: chunk_id, end_val_mysql: pk_extract_values(end_pk_row, tableInfos[j], true), end_val_oth: pk_extract_values(end_pk_row, tableInfos[j], false)}
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
				sql_vals_pk := generateValuesForPredicat(tableInfos[j].param_indices_browser_next_qry, pk_extract_values(start_pk_row, tableInfos[j], true))
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
				end_pk_row = make([]string, resultset_pk_size)
				for n := range a_sql_row {
					end_pk_row[n] = a_sql_row[n].String
				}
				if mode_debug {
					log.Printf("tableChunkBrowser  [%02d]: table %s query :  %s \n with %d params\nval for query: %s\nresult: %s\n", id, tableInfos[j].fullName, the_finish_query, len(sql_vals_pk), sql_vals_pk, end_pk_row)
					log.Printf("tableChunkBrowser  [%02d]: table %s pk interval : %s -> %s\n", id, tableInfos[j].fullName, start_pk_row, end_pk_row)
					log.Printf("tableChunkBrowser  [%02d]: sizeofchunk %6d pk_cnt %6d start equal end %t\n", id, sizeofchunk, pk_cnt, reflect.DeepEqual(start_pk_row, end_pk_row))
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
			a_chunk.begin_val_mysql = pk_extract_values(start_pk_row, tableInfos[j], true)
			a_chunk.begin_val_oth = pk_extract_values(start_pk_row, tableInfos[j], false)
			if !begin_equal_end {
				a_chunk.end_val_mysql = pk_extract_values(end_pk_row, tableInfos[j], true)
				a_chunk.end_val_oth = pk_extract_values(end_pk_row, tableInfos[j], false)
				stats2push <- action2Stat{Action: Action_Read, Phase: Phase_Browser, TabId: j, Cnt: pk_cnt - 1}
			} else {
				stats2push <- action2Stat{Action: Action_Read, Phase: Phase_Browser, TabId: j, Cnt: pk_cnt}
			}
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
		log.Printf("tableChunkBrowser [%02d]: finish\b", id)
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
func ChunkReaderProcess(threadid int, q_rows *sql.Rows, a_table_info *MetadataTable, tch *tablechunk) *datachunk {
	// --------------------------------------------------------------------------
	var a_dta_chunk datachunk
	a_dta_chunk = datachunk{tchunk: tch, rows: make([]*rowchunk, 0)}
	if mode_debug {
		log.Printf("ChunkReaderProcess [%02d]: table %03d chunk %12d \n", threadid, tch.table_id, tch.chunk_id)
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
			log.Printf("can not scan %s , len(ptrs) = %d , cols %s ", a_table_info.tbName, len(ptrs))
			log.Fatal(err)
		}
		// ------------------------------------------------------------------
		a_dta_chunk.rows = append(a_dta_chunk.rows, &a_simple_row)
		// ------------------------------------------------------------------
	}
	if mode_debug {
		log.Printf("ChunkReaderProcess [%02d]: table %03d chunk %12d len %5d\n", threadid, tch.table_id, tch.chunk_id, len(a_dta_chunk.rows))
	}
	return &a_dta_chunk
}

// ------------------------------------------------------------------------------------------
type cacheTableChunkReader struct {
	table_id                  int
	lastusagecnt              int
	fullname                  string
	indices_lo_pk             []int
	indices_up_pk             []int
	interval_query            string
	lower_bound_query         string
	upper_bound_query         string
	interval_prepared_stmt    *sql.Stmt
	lower_bound_prepared_stmt *sql.Stmt
	upper_bound_prepared_stmt *sql.Stmt
}

// ------------------------------------------------------------------------------------------
func tableChunkReader(chunk2read chan tablechunk, chan2dstreader chan datachunk, adbConn *sql.Conn, tableInfos []MetadataTable, id int, cntBrowser int, stats2push chan action2Stat) {
	if mode_debug {
		log.Printf("tableChunkReader   [%02d]: start\n", id)
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
		if a_chunk.table_id == -1 {
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
					tabReadingVars[lru_slot].lower_bound_prepared_stmt.Close()
					tabReadingVars[lru_slot].upper_bound_prepared_stmt.Close()
					tabReadingVars[lru_slot] = nil
					empty_slot = lru_slot
				}
				// ----------------------------------------------------------
				var new_info cacheTableChunkReader

				new_info.table_id = a_chunk.table_id
				new_info.lastusagecnt = cntreadchunk

				new_info.fullname = tableInfos[a_chunk.table_id].fullName
				new_info.interval_query = tableInfos[a_chunk.table_id].query_for_reader_interval
				new_info.lower_bound_query = tableInfos[a_chunk.table_id].query_for_reader_lower_bound
				new_info.upper_bound_query = tableInfos[a_chunk.table_id].query_for_reader_upper_bound
				new_info.indices_lo_pk = tableInfos[a_chunk.table_id].param_indices_interval_lo_qry
				new_info.indices_up_pk = tableInfos[a_chunk.table_id].param_indices_interval_up_qry
				// ------------------------------------------------------------------
				ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
				var p_err error
				new_info.interval_prepared_stmt, p_err = adbConn.PrepareContext(ctx, new_info.interval_query)
				if p_err != nil {
					log.Printf("tableChunkReader   [%02d]: finish cntreadchunk:%9d last_hit: %9d cache_hit: %9d cache_miss:%9d\n", id, cntreadchunk, last_hit, cache_hit, cache_miss)
					log.Fatalf("can not prepare to read a chunk ( %s )\n%s", new_info.interval_query, p_err.Error())
				}
				ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
				new_info.lower_bound_prepared_stmt, p_err = adbConn.PrepareContext(ctx, new_info.lower_bound_query)
				if p_err != nil {
					log.Printf("tableChunkReader   [%02d]: finish cntreadchunk:%9d last_hit: %9d cache_hit: %9d cache_miss:%9d\n", id, cntreadchunk, last_hit, cache_hit, cache_miss)
					log.Fatalf("can not prepare to read a lower bounded chunk ( %s )\n%s", new_info.lower_bound_query, p_err.Error())
				}
				ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
				new_info.upper_bound_prepared_stmt, p_err = adbConn.PrepareContext(ctx, new_info.upper_bound_query)
				if p_err != nil {
					log.Printf("tableChunkReader   [%02d]: finish cntreadchunk:%9d last_hit: %9d cache_hit: %9d cache_miss:%9d\n", id, cntreadchunk, last_hit, cache_hit, cache_miss)
					log.Fatalf("can not prepare to read a upper bounded chunk ( %s )\n%s", new_info.upper_bound_query, p_err.Error())
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
			log.Printf("tableChunkReader   [%02d]: table %03d chunk %12d %s -> %s\n", id, a_chunk.table_id, a_chunk.chunk_id, a_chunk.begin_val_oth, a_chunk.end_val_oth)
		}
		// --------------------------------------------------------------------------
		var sql_vals_pk []any
		var the_query *string
		var prepared_query *sql.Stmt
		if a_chunk.begin_val_mysql != nil {
			if a_chunk.end_val_mysql != nil {
				sql_vals_pk = generateValuesForPredicat(last_table.indices_lo_pk, a_chunk.begin_val_mysql)
				sql_vals_pk = append(sql_vals_pk, generateValuesForPredicat(last_table.indices_up_pk, a_chunk.end_val_mysql)...)
				the_query = &last_table.interval_query
				prepared_query = last_table.interval_prepared_stmt
			} else {
				sql_vals_pk = generateValuesForPredicat(last_table.indices_lo_pk, a_chunk.begin_val_mysql)
				the_query = &last_table.lower_bound_query
				prepared_query = last_table.lower_bound_prepared_stmt
			}
		} else {
			sql_vals_pk = generateValuesForPredicat(last_table.indices_up_pk, a_chunk.end_val_mysql)
			the_query = &last_table.upper_bound_query
			prepared_query = last_table.upper_bound_prepared_stmt
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
		dchunk := ChunkReaderProcess(id, q_rows, &tableInfos[last_table.table_id], &a_chunk)
		stats2push <- action2Stat{Action: Action_Read, Phase: Phase_SrcReader, TabId: last_table.table_id, Cnt: len(dchunk.rows)}
		chan2dstreader <- *dchunk
		// --------------------------------------------------------------------------
		if mode_debug {
			log.Printf("table %s chunk %12d readrows %5d query :  %s \n with %d params\nval for query: %s\n", last_table.fullname, a_chunk.chunk_id, len(dchunk.rows), *the_query, len(sql_vals_pk), sql_vals_pk)
		}
		// --------------------------------------------------------------------------
	}
	// ----------------------------------------------------------------------------------
	for n := range tabReadingVars {
		if tabReadingVars[n] != nil {
			tabReadingVars[n].interval_prepared_stmt.Close()
			tabReadingVars[n].lower_bound_prepared_stmt.Close()
			tabReadingVars[n].upper_bound_prepared_stmt.Close()
		}
	}
	// ----------------------------------------------------------------------------------
	if mode_debug {
		log.Printf("tableChunkReader  [%02d]: finish cntreadchunk:%9d last_hit: %9d cache_hit: %9d cache_miss:%9d\n", id, cntreadchunk, last_hit, cache_hit, cache_miss)
		log.Printf("tableChunkReader  [%02d]: finish\n", id)
	}
}

// ------------------------------------------------------------------------------------------
func tableDstChunkReader(chunk2read chan datachunk, chan2comparator chan datachunkpair, adbConn *sql.Conn, tableInfos []MetadataTable, id int, cntBrowser int, driverDatabase string, stats2push chan action2Stat) {
	if mode_debug {
		log.Printf("tableDstChunkReader[%02d]: start\n", id)
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
		if a_chunk.tchunk == nil {
			break
		}
		cntreadchunk++

		if last_table == nil || last_table.table_id != a_chunk.tchunk.table_id {
			// ------------------------------------------------------------------
			var tab_found int = -1
			var empty_slot int = -1
			var lru_slot int = -1
			var lru_val int = math.MaxInt
			for n := range tabReadingVars {
				if tabReadingVars[n] == nil {
					empty_slot = n
				} else if tabReadingVars[n].table_id == a_chunk.tchunk.table_id {
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
					tabReadingVars[lru_slot].lower_bound_prepared_stmt.Close()
					tabReadingVars[lru_slot].upper_bound_prepared_stmt.Close()
					tabReadingVars[lru_slot] = nil
					empty_slot = lru_slot
				}
				// ----------------------------------------------------------
				var new_info cacheTableChunkReader

				new_info.table_id = a_chunk.tchunk.table_id
				new_info.lastusagecnt = cntreadchunk

				new_info.fullname = tableInfos[a_chunk.tchunk.table_id].fullName
				new_info.interval_query = tableInfos[a_chunk.tchunk.table_id].query_for_dst_reader_interval
				new_info.lower_bound_query = tableInfos[a_chunk.tchunk.table_id].query_for_dst_reader_lower_bound
				new_info.upper_bound_query = tableInfos[a_chunk.tchunk.table_id].query_for_dst_reader_upper_bound
				new_info.indices_lo_pk = tableInfos[a_chunk.tchunk.table_id].param_indices_interval_lo_qry
				new_info.indices_up_pk = tableInfos[a_chunk.tchunk.table_id].param_indices_interval_up_qry
				// ------------------------------------------------------------------
				ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
				var p_err error
				new_info.interval_prepared_stmt, p_err = adbConn.PrepareContext(ctx, new_info.interval_query)
				if p_err != nil {
					log.Printf("tableDstChunkReader[%02d]: finish cntreadchunk:%9d last_hit: %9d cache_hit: %9d cache_miss:%9d\n", id, cntreadchunk, last_hit, cache_hit, cache_miss)
					log.Fatalf("can not prepare to read a chunk ( %s )\n%s", new_info.interval_query, p_err.Error())
				}
				ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
				new_info.lower_bound_prepared_stmt, p_err = adbConn.PrepareContext(ctx, new_info.lower_bound_query)
				if p_err != nil {
					log.Printf("tableDstChunkReader[%02d]: finish cntreadchunk:%9d last_hit: %9d cache_hit: %9d cache_miss:%9d\n", id, cntreadchunk, last_hit, cache_hit, cache_miss)
					log.Fatalf("can not prepare to read a lower bounded chunk ( %s )\n%s", new_info.lower_bound_query, p_err.Error())
				}
				ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
				new_info.upper_bound_prepared_stmt, p_err = adbConn.PrepareContext(ctx, new_info.upper_bound_query)
				if p_err != nil {
					log.Printf("tableDstChunkReader[%02d]: finish cntreadchunk:%9d last_hit: %9d cache_hit: %9d cache_miss:%9d\n", id, cntreadchunk, last_hit, cache_hit, cache_miss)
					log.Fatalf("can not prepare to read a upper bounded chunk ( %s )\n%s", new_info.upper_bound_query, p_err.Error())
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
			if driverDatabase == "mysql" {
				log.Printf("tableDstChunkReader[%02d]: table %03d chunk %12d %s -> %s\n", id, a_chunk.tchunk.table_id, a_chunk.tchunk.chunk_id, a_chunk.tchunk.begin_val_mysql, a_chunk.tchunk.end_val_mysql)
			} else {
				log.Printf("tableDstChunkReader[%02d]: table %03d chunk %12d %s -> %s\n", id, a_chunk.tchunk.table_id, a_chunk.tchunk.chunk_id, a_chunk.tchunk.begin_val_oth, a_chunk.tchunk.end_val_oth)
			}
		}
		// --------------------------------------------------------------------------
		var sql_vals_pk []any
		var the_query *string
		var prepared_query *sql.Stmt

		if a_chunk.tchunk.begin_val_mysql != nil {
			if a_chunk.tchunk.end_val_mysql != nil {
				if driverDatabase == "mysql" {
					sql_vals_pk = generateValuesForPredicat(last_table.indices_lo_pk, a_chunk.tchunk.begin_val_mysql)
					sql_vals_pk = append(sql_vals_pk, generateValuesForPredicat(last_table.indices_up_pk, a_chunk.tchunk.end_val_mysql)...)
				} else {
					sql_vals_pk = generateValuesForPredicat(last_table.indices_lo_pk, a_chunk.tchunk.begin_val_oth)
					sql_vals_pk = append(sql_vals_pk, generateValuesForPredicat(last_table.indices_up_pk, a_chunk.tchunk.end_val_oth)...)
				}
				the_query = &last_table.interval_query
				prepared_query = last_table.interval_prepared_stmt
			} else {
				if driverDatabase == "mysql" {
					sql_vals_pk = generateValuesForPredicat(last_table.indices_lo_pk, a_chunk.tchunk.begin_val_mysql)
				} else {
					sql_vals_pk = generateValuesForPredicat(last_table.indices_lo_pk, a_chunk.tchunk.begin_val_oth)
				}
				the_query = &last_table.lower_bound_query
				prepared_query = last_table.lower_bound_prepared_stmt
			}
		} else {
			if driverDatabase == "mysql" {
				sql_vals_pk = generateValuesForPredicat(last_table.indices_up_pk, a_chunk.tchunk.end_val_mysql)
			} else {
				sql_vals_pk = generateValuesForPredicat(last_table.indices_up_pk, a_chunk.tchunk.end_val_oth)
			}
			the_query = &last_table.upper_bound_query
			prepared_query = last_table.upper_bound_prepared_stmt
		}
		q_rows, q_err := prepared_query.Query(sql_vals_pk...)
		if q_err != nil {
			log.Printf("tableDstChunkReader[%02d]: table %s chunk id: %12d chunk query :  %s \n with %d params\nval for query: %s\n", id, last_table.fullname, a_chunk.tchunk.chunk_id, *the_query, len(sql_vals_pk), sql_vals_pk)
			log.Printf("tableDstChunkReader[%02d]: ind lo: %s  ind up: %s", id, last_table.indices_lo_pk, last_table.indices_up_pk)
			log.Fatalf("tableDstChunkReader[%02d]: can not query table %s to read the chunks\n%s", id, last_table.fullname, sql_vals_pk, q_err.Error())
		}
		if mode_debug {
			log.Printf("tableDstChunkReader[%02d]: table %s chunk id: %12d chunk query :  %s \n with %d params\nval for query: %s\n", id, last_table.fullname, a_chunk.tchunk.chunk_id, *the_query, len(sql_vals_pk), sql_vals_pk)
			log.Printf("tableDstChunkReader[%02d]: ind lo: %s  ind up: %s", id, last_table.indices_lo_pk, last_table.indices_up_pk)
		}
		// --------------------------------------------------------------------------
		n_chunk := ChunkReaderProcess(id, q_rows, &tableInfos[last_table.table_id], a_chunk.tchunk)
		stats2push <- action2Stat{Action: Action_Read, Phase: Phase_DstReader, TabId: last_table.table_id, Cnt: len(n_chunk.rows)}
		chan2comparator <- datachunkpair{c_src: &a_chunk, c_dst: n_chunk}
		// --------------------------------------------------------------------------
		if mode_debug {
			log.Printf("tableDstChunkReader[%02d]: table %s chunk query :  %s \n with %d params\nval for query: %s\n", id, last_table.fullname, *the_query, len(sql_vals_pk), sql_vals_pk)
		}
		// --------------------------------------------------------------------------
	}
	// ----------------------------------------------------------------------------------
	for n := range tabReadingVars {
		if tabReadingVars[n] != nil {
			tabReadingVars[n].interval_prepared_stmt.Close()
			tabReadingVars[n].lower_bound_prepared_stmt.Close()
			tabReadingVars[n].upper_bound_prepared_stmt.Close()
		}
	}
	// ----------------------------------------------------------------------------------
	if mode_debug {
		log.Printf("tableDstChunkReader[%02d]: finish cntreadchunk:%9d last_hit: %9d cache_hit: %9d cache_miss:%9d\n", id, cntreadchunk, last_hit, cache_hit, cache_miss)
		log.Printf("tableDstChunkReader[%02d]: finish\n", id)
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
		a_str = a_str + " `" + col_pk[ctab] + "` "
		if col_is_enum {
			a_str = a_str + " , cast(`" + col_pk[ctab] + "` as unsigned integer ) as `integer_of_" + col_pk[ctab] + "`"
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
// mssql : we need only to quote the single quote
var quote_substitute_string_mssql = [256]uint8{
	//    1    2    3    4     5    6   7    8    9    A    B    C    D    E    F
	'C', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'C', 'A', 'A', 'C', 'A', 'A', // 0x00-0x0F
	'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'C', 'A', 'A', 'A', 'A', 'A', // 0x10-0x1F
	'A', 'A', 'A', 'A', 'A', 'A', 'A', 'C', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', // 0x20-0x2F
	'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', // 0x30-0x3F
	'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', // 0x40-0x4F
	'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', // 0x50-0x5F
	'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', // 0x60-0x6F
	'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', // 0x70-0x7F
	//    1    2    3    4     5    6    7    8    9    A    B     C    D    E    F
	'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', // 0x80-0x8F
	'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', // 0x90-0x9F
	'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', // 0xA0-0xAF
	'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', // 0xB0-0xBF
	'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', // 0xC0-0xCF
	'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', // 0xD0-0xDF
	'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', // 0xE0-0xEF
	'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', 'U', // 0xF0-0xFF
}

func stringIsASCII(s_ptr *string) bool {
	s_len := len(*s_ptr)
	if s_len == 0 {
		return true
	}
	b_pos := 0
	var new_char byte
	for {
		first_char := (*s_ptr)[b_pos]
		new_char = quote_substitute_string_mssql[first_char]
		if new_char != 'A' {
			return false
		}
		b_pos++
		if b_pos == s_len {
			return true
		}
	}
}

func needCopyForquoteStringMsSql(s_ptr *string) (*string, int, int, byte) {
	s_len := len(*s_ptr)
	if s_len == 0 {
		return s_ptr, 0, -1, 0
	}
	b_pos := 0
	var new_char byte
	for {
		first_char := (*s_ptr)[b_pos]
		new_char = quote_substitute_string_mssql[first_char]
		if new_char != 'A' {
			return s_ptr, s_len, b_pos, new_char
		}
		b_pos++
		if b_pos == s_len {
			return s_ptr, s_len, -1, 0
		}
	}
}

func quoteStringFromPosMsSql(s_ptr *string, s_len int, b_pos int, new_char byte) (*string, int) {
	var new_str strings.Builder
	need_plus := false
	mode_n := false
	new_str.WriteString((*s_ptr)[:b_pos])
	if new_char == 'C' {
		new_str.WriteString(fmt.Sprintf("'+CHAR(%d)", (*s_ptr)[b_pos]))
		need_plus = true
	}
	if new_char == 'U' {
		new_str.WriteString("'+N'")
		new_str.WriteByte((*s_ptr)[b_pos])
		need_plus = false
		mode_n = true
	}
	b_pos++
	for b_pos < s_len {
		first_char := (*s_ptr)[b_pos]
		new_char = quote_substitute_string_mssql[first_char]
		if new_char == 'A' {
			if need_plus {
				new_str.WriteString("+'")
				need_plus = false
			}
			new_str.WriteByte(first_char)
		} else if new_char == 'C' {
			if need_plus {
				new_str.WriteString(fmt.Sprintf("+CHAR(%d)", new_char))
			} else {
				new_str.WriteString(fmt.Sprintf("'+CHAR(%d)", new_char))
			}
			need_plus = true
			mode_n = false
		} else if new_char == 'U' {
			if need_plus {
				new_str.WriteString("+N'")
				need_plus = false
			} else {
				if !mode_n {
					new_str.WriteString("'+N'")
					mode_n = true
				}
				new_str.WriteByte(first_char)
			}
		}
		b_pos++
	}
	if need_plus {
		new_str.WriteString("+'")
	}
	n_str := new_str.String()
	return &n_str, len(n_str)
}

// ------------------------------------------------------------------------------------------
func compareRow(src_r *rowchunk, dst_r *rowchunk, colspk int, colscnt int, colskinddate *[]bool) (int, int) {
	for c := 0; c < colspk; c++ {
		if src_r.cols[c].Valid {
			if dst_r.cols[c].Valid {
				if mode_trace {
					log.Printf("compareRow col %2d VAL src %-40s dst %-40s", c, src_r.cols[c].String, dst_r.cols[c].String)
				}
				if (*colskinddate)[c] {
					t1, err1 := time.Parse(time.DateTime, src_r.cols[c].String)
					if err1 != nil {
						t1, err1 = time.Parse(time.RFC3339, src_r.cols[c].String)
						if err1 != nil {
							log.Fatalf("can convert this '%s' to time.Time", src_r.cols[c].String)
						}
					}
					t2, err2 := time.Parse(time.DateTime, dst_r.cols[c].String)
					if err2 != nil {
						t2, err2 = time.Parse(time.RFC3339, dst_r.cols[c].String)
						if err2 != nil {
							log.Fatalf("can convert this '%s' to time.Time", src_r.cols[c].String)
						}
					}
					if t1.Before(t2) {
						return -1, -1
					}
					if t1.After(t2) {
						return 1, 1
					}
				} else {
					if src_r.cols[c].String < dst_r.cols[c].String {
						return -1, -1
					}
					if src_r.cols[c].String > dst_r.cols[c].String {
						return 1, 1
					}
				}
			} else {
				return 1, 1
			}
		} else {
			if dst_r.cols[c].Valid {
				return -1, -1
			}
		}
	}

	for c := colspk; c < colscnt; c++ {
		if src_r.cols[c].Valid {
			if dst_r.cols[c].Valid {
				if mode_trace {
					log.Printf("compareRow col %2d VAL src %-40s/%-80s dst %-40s %-80s", c, src_r.cols[c].String, dst_r.cols[c].String, hex.EncodeToString([]byte(src_r.cols[c].String)), hex.EncodeToString([]byte(dst_r.cols[c].String)))
				}
				if (*colskinddate)[c] {
					t1, err1 := time.Parse(time.DateTime, src_r.cols[c].String)
					if err1 != nil {
						t1, err1 = time.Parse(time.RFC3339, src_r.cols[c].String)
						if err1 != nil {
							log.Fatalf("can convert this '%s' to time.Time", src_r.cols[c].String)
						}
					}
					t2, err2 := time.Parse(time.DateTime, dst_r.cols[c].String)
					if err2 != nil {
						t2, err2 = time.Parse(time.RFC3339, dst_r.cols[c].String)
						if err2 != nil {
							log.Fatalf("can convert this '%s' to time.Time", src_r.cols[c].String)
						}
					}
					if t1.Before(t2) {
						return -1, -1
					}
					if t1.After(t2) {
						return 1, 1
					}
				} else {
					if src_r.cols[c].String < dst_r.cols[c].String {
						return 0, -1
					}
					if src_r.cols[c].String > dst_r.cols[c].String {
						return 0, 1
					}
				}
			} else {
				return 0, 1
			}
		} else {
			if dst_r.cols[c].Valid {
				return 0, -1
			}
		}
	}
	return 0, 0
}

// ------------------------------------------------------------------------------------------
func dataChunkComparator(rowvalueschan chan datachunkpair, id int, tableInfos []MetadataTable, sql2generate chan datarow, dst_driver string, cntBrowser int) {
	// ----------------------------------------------------------------------------------
	for {
		pair_chunks := <-rowvalueschan
		if pair_chunks.c_src == nil && pair_chunks.c_dst == nil {
			break
		}
		// --------------------------------------------------------------------------
		cntCols := tableInfos[pair_chunks.c_src.tchunk.table_id].cntCols
		cntPkCols := tableInfos[pair_chunks.c_src.tchunk.table_id].cntPkCols
		if mode_trace {
			if dst_driver == "mysql" {
				log.Printf("dataChunkComparator[%02d]: chunk %d begin pk %s end pk %s ", id, pair_chunks.c_src.tchunk.chunk_id, pair_chunks.c_src.tchunk.begin_val_mysql, pair_chunks.c_src.tchunk.end_val_mysql)
			} else {
				log.Printf("dataChunkComparator[%02d]: chunk %d begin pk %s end pk %s ", id, pair_chunks.c_src.tchunk.chunk_id, pair_chunks.c_src.tchunk.begin_val_oth, pair_chunks.c_src.tchunk.end_val_oth)
			}
			log.Printf("dataChunkComparator[%02d]: chunk %d rows src %05d dst %05d ", id, pair_chunks.c_src.tchunk.chunk_id, len(pair_chunks.c_src.rows), len(pair_chunks.c_dst.rows))
		}
		// ---------------
		colIsDate := make([]bool, cntCols)
		for i, v := range tableInfos[pair_chunks.c_src.tchunk.table_id].columnInfos {
			colIsDate[i] = v.isKindDate
		}
		// --------------------------------------------------------------------------
		sort.SliceStable(pair_chunks.c_src.rows,
			func(i, j int) bool {
				for c := 0; c < cntCols; c++ {
					if pair_chunks.c_src.rows[i].cols[c].Valid {
						if pair_chunks.c_src.rows[j].cols[c].Valid {
							if pair_chunks.c_src.rows[i].cols[c].String < pair_chunks.c_src.rows[j].cols[c].String {
								return true
							}
							if pair_chunks.c_src.rows[i].cols[c].String > pair_chunks.c_src.rows[j].cols[c].String {
								return false
							}
						} else {
							return false
						}
					} else {
						if pair_chunks.c_src.rows[j].cols[c].Valid {
							return true
						}
					}
				}
				return false
			})
		sort.SliceStable(pair_chunks.c_dst.rows,
			func(i, j int) bool {
				for c := 0; c < cntCols; c++ {
					if pair_chunks.c_dst.rows[i].cols[c].Valid {
						if pair_chunks.c_dst.rows[j].cols[c].Valid {
							if pair_chunks.c_dst.rows[i].cols[c].String < pair_chunks.c_dst.rows[j].cols[c].String {
								return true
							}
							if pair_chunks.c_dst.rows[i].cols[c].String > pair_chunks.c_dst.rows[j].cols[c].String {
								return false
							}
						} else {
							return false
						}
					} else {
						if pair_chunks.c_dst.rows[j].cols[c].Valid {
							return true
						}
					}
				}
				return false
			})
		// --------------------------------------------------------------------------
		cur_src, cur_dst := 0, 0
		for cur_src < len(pair_chunks.c_src.rows) || cur_dst < len(pair_chunks.c_dst.rows) {
			// ------------------------------------------------------------------
			if mode_trace {
				log.Printf("dataChunkComparator[%02d]: chunk %d  | cur src %5d dst %5d | len src %5d dst %5d", id, pair_chunks.c_src.tchunk.chunk_id, cur_src, cur_dst, len(pair_chunks.c_src.rows), len(pair_chunks.c_dst.rows))
			}
			// ------------------------------------------------------------------
			if cur_src == len(pair_chunks.c_src.rows) {
				sql2generate <- datarow{table_id: pair_chunks.c_src.tchunk.table_id, dmltype: DML_Delete, whrdta: pair_chunks.c_dst.rows[cur_dst]}
				cur_dst++
				continue
			}
			if cur_dst == len(pair_chunks.c_dst.rows) {
				sql2generate <- datarow{table_id: pair_chunks.c_src.tchunk.table_id, dmltype: DML_Insert, valdta: pair_chunks.c_src.rows[cur_src]}
				cur_src++
				continue
			}
			// ------------------------------------------------------------------
			cmp_pk, cmp_row := compareRow(pair_chunks.c_src.rows[cur_src], pair_chunks.c_dst.rows[cur_dst], cntPkCols, cntCols, &colIsDate)
			// ------------------------------------------------------------------
			if mode_trace {
				log.Printf("dataChunkComparator[%02d]: chunk %d  | cur src %5d dst %5d | len src %5d dst %5d | cmp pk %2d whole %2d ", id, pair_chunks.c_src.tchunk.chunk_id, cur_src, cur_dst, len(pair_chunks.c_src.rows), len(pair_chunks.c_dst.rows), cmp_pk, cmp_row)
			}
			// ------------------------------------------------------------------
			if cmp_pk == 0 && cmp_row == 0 {
				cur_src++
				cur_dst++
				continue
			}
			// ------------------------------------------------------------------
			if cmp_pk == -1 {
				sql2generate <- datarow{table_id: pair_chunks.c_src.tchunk.table_id, dmltype: DML_Insert, valdta: pair_chunks.c_src.rows[cur_src]}
				cur_src++
				continue
			}
			// ------------------------------------------------------------------
			if cmp_pk == 1 {
				sql2generate <- datarow{table_id: pair_chunks.c_src.tchunk.table_id, dmltype: DML_Delete, whrdta: pair_chunks.c_dst.rows[cur_dst]}
				cur_dst++
				continue
			}
			if cmp_pk == 0 && cmp_row != 0 {
				sql2generate <- datarow{table_id: pair_chunks.c_src.tchunk.table_id, dmltype: DML_Update, valdta: pair_chunks.c_src.rows[cur_src], whrdta: pair_chunks.c_dst.rows[cur_dst]}
				cur_src++
				cur_dst++
				continue
			}
			// ------------------------------------------------------------------
		}
		// --------------------------------------------------------------------------
	}
	// ----------------------------------------------------------------------------------
}

// ------------------------------------------------------------------------------------------
func dataSqlGenerator(rowtomodify chan datarow, id int, tableInfos []MetadataTable, sql2execute chan insertchunk, dst_driver string, cntBrowser int) {
	// ----------------------------------------------------------------------------------
	var param_pattern string
	var where_param_pattern string
	if dst_driver == "mysql" {
		param_pattern = "?"
		where_param_pattern = "= ?"
	}
	if dst_driver == "postgres" {
		param_pattern = "$"
		where_param_pattern = "= $"
	}
	if dst_driver == "mssql" {
		param_pattern = "@p"
		where_param_pattern = "= @p"
	}
	// ----------------------------------------------------------------------------------
	for {
		a_dml_row := <-rowtomodify
		if a_dml_row.table_id == -1 {
			break
		}
		// --------------------------------------------------------------------------
		no_null_cnt := 0
		no_null_whr_cnt := 0
		if a_dml_row.dmltype == DML_Insert || a_dml_row.dmltype == DML_Update {
			for _, v := range a_dml_row.valdta.cols {
				if v.Valid {
					no_null_cnt += 1
				}
			}
		}
		if a_dml_row.dmltype == DML_Update || a_dml_row.dmltype == DML_Delete {
			for _, v := range a_dml_row.whrdta.cols {
				if v.Valid {
					no_null_whr_cnt += 1
				}
			}
		}
		var sqlParams []any = make([]any, no_null_cnt+no_null_whr_cnt)
		var qryPlaceHolders []string = make([]string, no_null_cnt+no_null_whr_cnt)
		// ----------------------------------------------------------
		q_siz := 0
		if dst_driver == "mysql" {
			param_pos := 0
			if a_dml_row.dmltype == DML_Insert || a_dml_row.dmltype == DML_Update {
				for _, v := range a_dml_row.valdta.cols {
					if !v.Valid {
						qryPlaceHolders[param_pos] = "NULL"
						q_siz += 4
					} else {
						qryPlaceHolders[param_pos] = param_pattern
						sqlParams[param_pos] = v.String
						param_pos += 1
						q_siz += 1
					}
				}
			}
			if a_dml_row.dmltype == DML_Update || a_dml_row.dmltype == DML_Delete {
				for _, v := range a_dml_row.whrdta.cols {
					if !v.Valid {
						qryPlaceHolders[param_pos] = " IS NULL"
						q_siz += 8
					} else {
						qryPlaceHolders[param_pos] = where_param_pattern
						sqlParams[param_pos] = v.String
						param_pos += 1
						q_siz += 3
					}
				}
			}
		} else {
			param_pos := 0
			if a_dml_row.dmltype == DML_Insert || a_dml_row.dmltype == DML_Update {
				for n, v := range a_dml_row.valdta.cols {
					if !v.Valid {
						qryPlaceHolders[param_pos] = "NULL"
						q_siz += 4
					} else {
						str_val := param_pattern + strconv.Itoa(param_pos+1)
						qryPlaceHolders[param_pos] = str_val
						if tableInfos[a_dml_row.table_id].columnInfos[n].isKindBinary {
							sqlParams[param_pos] = []byte(v.String)
						} else {
							if dst_driver == "postgres" && strings.IndexByte(v.String, 0) != -1 {
								sqlParams[param_pos] = strings.ReplaceAll(v.String, "\x00", "")
							} else {
								sqlParams[param_pos] = v.String
							}
						}
						param_pos += 1
						q_siz += len(str_val)
					}
				}
			}
			if a_dml_row.dmltype == DML_Update || a_dml_row.dmltype == DML_Delete {
				for n, v := range a_dml_row.whrdta.cols {
					if !v.Valid {
						qryPlaceHolders[param_pos] = " IS NULL"
						q_siz += 8
					} else {
						str_val := where_param_pattern + strconv.Itoa(param_pos+1)
						qryPlaceHolders[param_pos] = str_val
						if tableInfos[a_dml_row.table_id].columnInfos[n].isKindBinary {
							sqlParams[param_pos] = []byte(v.String)
						} else {
							if dst_driver == "postgres" && strings.IndexByte(v.String, 0) != -1 {
								sqlParams[param_pos] = strings.ReplaceAll(v.String, "\x00", "")
							} else {
								sqlParams[param_pos] = v.String
							}
						}
						param_pos += 1
						q_siz += len(str_val)
					}
				}
			}
		}
		// --------------------------------------------------------------------------
		var b strings.Builder
		var estim_size int
		if a_dml_row.dmltype == DML_Insert {
			estim_size = tableInfos[a_dml_row.table_id].query_for_insert_size + q_siz
		}
		if a_dml_row.dmltype == DML_Update {
			estim_size = tableInfos[a_dml_row.table_id].query_for_update_size + q_siz
		}
		if a_dml_row.dmltype == DML_Delete {
			estim_size = tableInfos[a_dml_row.table_id].query_for_delete_size + q_siz
		}
		b.Grow(estim_size)
		if a_dml_row.dmltype == DML_Insert {
			if mode_trace {
				log.Printf("dataSqlGenerator[%d] size q_ins %d size p_hld %d\n", id, len(tableInfos[a_dml_row.table_id].query_for_insert), len(qryPlaceHolders))
			}
			b.WriteString(tableInfos[a_dml_row.table_id].query_for_insert[0])
			for n, v := range qryPlaceHolders {
				b.WriteString(v)
				b.WriteString(tableInfos[a_dml_row.table_id].query_for_insert[n+1])
			}
		}
		if a_dml_row.dmltype == DML_Update {
			b.WriteString(tableInfos[a_dml_row.table_id].query_for_update[0])
			for n, v := range qryPlaceHolders {
				b.WriteString(v)
				b.WriteString(tableInfos[a_dml_row.table_id].query_for_update[n+1])
			}
		}
		if a_dml_row.dmltype == DML_Delete {
			b.WriteString(tableInfos[a_dml_row.table_id].query_for_delete[0])
			for n, v := range qryPlaceHolders {
				b.WriteString(v)
				b.WriteString(tableInfos[a_dml_row.table_id].query_for_delete[n+1])
			}
		}
		// --------------------------------------------------------------------------
		a_str := b.String()
		sql2execute <- insertchunk{table_id: a_dml_row.table_id, sql: &a_str, params: &sqlParams, cntrows: 1, dmltype: a_dml_row.dmltype}
		// --------------------------------------------------------------------------
	}
	// ----------------------------------------------------------------------------------
}

// ------------------------------------------------------------------------------------------
func tableDstDbWriter(sql2inject chan insertchunk, adbConn *sql.Conn, id int, stats2push chan action2Stat, dont_do_insert bool, dont_do_update bool, dont_do_delete bool) {
	if mode_debug {
		log.Printf("tableDstDbWriter   [%02d]: start\n", id)
	}
	if dont_do_insert {
		log.Printf("tableDstDbWriter   [%02d]: do INSERT\n", id)
	}
	if dont_do_update {
		log.Printf("tableDstDbWriter   [%02d]: no UPDATE\n", id)
	}
	if dont_do_delete {
		log.Printf("tableDstDbWriter   [%02d]: no DELETE\n", id)
	}
	// ----------------------------------------------------------------------------------
	a_insert_sql := <-sql2inject
	for a_insert_sql.table_id != -1 {
		// --------------------------------------------------------------------------
		if mode_trace {
			log.Printf("tableDstDbWriter   [%02d]: dmltype %d cnt %d \n", id, a_insert_sql.dmltype, a_insert_sql.cntrows)
		}
		if (dont_do_insert && a_insert_sql.dmltype == DML_Insert) || (dont_do_update && a_insert_sql.dmltype == DML_Update) || (dont_do_delete && a_insert_sql.dmltype == DML_Delete) {
			stats2push <- action2Stat{Action: Action_NoOp, Phase: Phase_DstWriter, TabId: a_insert_sql.table_id, Cnt: a_insert_sql.cntrows}
		} else {
			if a_insert_sql.params != nil {
				_, e_err := adbConn.ExecContext(context.Background(), *a_insert_sql.sql, *a_insert_sql.params...)
				if e_err != nil {
					log.Printf("error with :\n%s", *a_insert_sql.sql)
					log.Printf("with params array of %d elems", len(*a_insert_sql.params))
					log.Fatalf("thread %d , can not insert/update/delete a chunk\n%s\n", id, e_err.Error())
				}
			} else {
				_, e_err := adbConn.ExecContext(context.Background(), *a_insert_sql.sql)
				if e_err != nil {
					log.Printf("error with :\n%s", *a_insert_sql.sql)
					log.Fatalf("thread %d , can not insert/update/delete a chunk\n%s\n", id, e_err.Error())
				}
			}
			stats2push <- action2Stat{Action: Action_Write, Phase: Phase_DstWriter, TabId: a_insert_sql.table_id, Cnt: a_insert_sql.cntrows}
		}
		// --------------------------------------------------------------------------
		a_insert_sql = <-sql2inject
	}
	// ----------------------------------------------------------------------------------
	if mode_debug {
		log.Printf("tableDstDbWriter   [%02d]: finish\n", id)
	}
}

// ------------------------------------------------------------------------------------------
func tableStatsAction(stat2summarize chan action2Stat, tableInfos []MetadataTable, filestat string) {
	// ----------------------------------------------------------------------------------
	if mode_debug {
		log.Print("tableStatsAction   [00]: start\n")
	}
	all_stats := make([]action2Stat, 0)
	for j := range tableInfos {
		all_stats = append(all_stats, action2Stat{Action: Action_Read, Phase: Phase_Browser, TabId: j, Cnt: 0})
	}
	var stats_file *os.File
	var err error
	if len(filestat) > 0 {
		stats_file, err = os.OpenFile(filestat, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			log.Printf("can not openfile %s", filestat)
			log.Fatal(err.Error())
		}
	}
	// ----------------------------------------------------------------------------------
	stat2add := <-stat2summarize
	for stat2add.TabId != -1 {
		// --------------------------------------------------------------------------
		s_found := -1
		for i, s := range all_stats {
			if s.Action == stat2add.Action && s.Phase == stat2add.Phase && s.TabId == stat2add.TabId {
				s_found = i
				break
			}
		}
		if s_found == -1 {
			all_stats = append(all_stats, stat2add)
		} else {
			all_stats[s_found].Cnt = all_stats[s_found].Cnt + stat2add.Cnt
		}
		// --------------------------------------------------------------------------
		stat2add = <-stat2summarize
	}
	// ----------------------------------------------------------------------------------
	deja_vu_table := make([]int, 0)
	for _, s := range all_stats {
		// --------------------------------------------------------------------------
		t_found := false
		for _, t := range deja_vu_table {
			if t == s.TabId {
				t_found = true
				break
			}
		}
		// --------------------------------------------------------------------------
		if !t_found {
			src_brow := 0
			src_read := 0
			dst_read := 0
			dst_writ := 0
			dst_noop := 0
			for _, sa := range all_stats {
				if sa.TabId == s.TabId {
					if sa.Action == Action_Read && sa.Phase == Phase_SrcReader {
						src_read = sa.Cnt
					} else {
						if sa.Action == Action_Read && sa.Phase == Phase_DstReader {
							dst_read = sa.Cnt
						} else {
							if sa.Action == Action_Write && sa.Phase == Phase_DstWriter {
								dst_writ = sa.Cnt
							} else {
								if sa.Action == Action_Read && sa.Phase == Phase_Browser {
									src_brow = sa.Cnt
								} else {
									if sa.Action == Action_NoOp && sa.Phase == Phase_DstWriter {
										dst_noop = sa.Cnt
									}
								}
							}
						}
					}
				}
			}
			stat_table := fmt.Sprintf("%-32s : src brw %9d read %9d / dst read %9d noop %9d wrt %9d\n", tableInfos[s.TabId].tbName, src_brow, src_read, dst_read, dst_noop, dst_writ)
			log.Printf("tableStatsAction   [00]: %s", stat_table)
			if len(filestat) > 0 {
				stats_file.WriteString(stat_table)
			}
			deja_vu_table = append(deja_vu_table, s.TabId)
		}
		// --------------------------------------------------------------------------
	}
	if mode_debug {
		log.Print("tableStatsAction   [00]: finish\n")
	}
	if len(filestat) > 0 {
		stats_file.Close()
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
	// ----------------------------------------------------------------------------------
	arg_db_driver := flag.String("driver", "mysql", "SQL engine , mysql / postgres / mssql")
	arg_db_port := flag.Int("port", 3306, "the database port")
	arg_db_host := flag.String("host", "127.0.0.1", "the database host")
	arg_db_user := flag.String("user", "mysql", "the database connection user")
	arg_db_pasw := flag.String("pwd", "", "the database connection password")
	arg_browser_parr := flag.Int("browser", 4, "number of browsers")
	arg_db_parr := flag.Int("readers", 10, "number of readers")
	arg_chunk_size := flag.Int("chunksize", 10000, "rows count when reading")
	arg_insert_size := flag.Int("insertsize", 500, "rows count for each insert")
	arg_dumpstats := flag.String("statsfile", "", "dump stats for activity on tables into a file")
	// ------------
	arg_db_name := flag.String("db", "", "the database to connect ( postgres & msqsql only) ")
	arg_dst_db_name := flag.String("dst-db", "", "the database to connect ( postgres & mssql only) ")
	// ------------
	arg_schema := flag.String("schema", "", "schema of table(s) to compare")
	arg_dst_schema := flag.String("dst-schema", "", "schema of table(s) on the destination db ")
	// ------------
	var arg_tables2sync arrayFlags
	flag.Var(&arg_tables2sync, "table", "table to compare")
	// ------------
	arg_guess_pk := flag.Bool("guessprimarykey", false, "guess a primary key in case table does not have one")
	// ------------
	arg_dst_db_driver := flag.String("dst-driver", "mysql", "SQL engine , mysql / postgres / mssql ")
	arg_dst_db_port := flag.Int("dst-port", 3306, "the database port")
	arg_dst_db_host := flag.String("dst-host", "127.0.0.1", "the database host")
	arg_dst_db_user := flag.String("dst-user", "mysql", "the database connection user")
	arg_dst_db_pasw := flag.String("dst-pwd", "", "the database connection password")
	arg_dst_db_read := flag.Int("dst-readers", 10, "number of readers")
	arg_dst_db_writ := flag.Int("dst-writers", 20, "number of writers")
	// ------------
	arg_dst_no_delete := flag.Bool("writer-no-delete", false, "disable delete on dst")
	arg_dst_no_update := flag.Bool("writer-no-update", false, "disable update on dst")
	arg_dst_no_insert := flag.Bool("writer-no-insert", false, "disable insert on dst")
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
	if arg_tables2sync == nil {
		log.Printf("no tables specified")
		flag.Usage()
		os.Exit(2)
	}
	if arg_schema == nil {
		log.Printf("no schema specified")
		flag.Usage()
		os.Exit(4)
	}
	if *arg_insert_size > *arg_chunk_size {
		log.Printf("invalid values for chunksize ,insertsize\n insertsize (%d) must be less or equal then chunksize (%d)", *arg_insert_size, *arg_chunk_size)
		flag.Usage()
		os.Exit(9)
	}
	if len(*arg_db_name) != 0 && (*arg_db_driver == "mysql") {
		flag.Usage()
		os.Exit(16)
	}
	if len(*arg_dst_db_name) != 0 && (*arg_dst_db_driver == "mysql") {
		flag.Usage()
		os.Exit(17)
	}
	if len(*arg_db_name) == 0 && ((*arg_db_driver == "postgres") || (*arg_db_driver == "mssql")) {
		flag.Usage()
		os.Exit(18)
	}
	if len(*arg_dst_db_name) == 0 && ((*arg_dst_db_driver == "postgres") || (*arg_dst_db_driver == "mssql")) {
		flag.Usage()
		os.Exit(19)
	}
	mode_trace = *arg_trace
	if mode_trace {
		mode_debug = true
	} else {
		mode_debug = *arg_debug
	}
	// ----------------------------------------------------------------------------------
	if arg_dst_schema == nil || len(*arg_dst_schema) == 0 {
		arg_dst_schema = arg_schema
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
		dbSrc, conSrc, _, _ = GetaSynchronizedMysqlConnections(*arg_db_host, *arg_db_port, *arg_db_user, *arg_db_pasw, cntReader+cntBrowser, *arg_schema)
	}
	if *arg_db_driver == "mssql" {
		dbSrc, conSrc, _, _ = GetaSynchronizedMsSqlConnections(*arg_db_host, *arg_db_port, *arg_db_user, *arg_db_pasw, cntReader+cntBrowser, *arg_schema)
	}
	if *arg_db_driver == "postgres" {
		dbSrc, conSrc, _, _ = GetaSynchronizedPostgresConnections(*arg_db_host, *arg_db_port, *arg_db_user, *arg_db_pasw, cntReader+cntBrowser, *arg_schema)
	}
	if *arg_dst_db_driver == "mysql" {
		dbDst, conDst, _ = GetDstMysqlConnections(*arg_dst_db_host, *arg_dst_db_port, *arg_dst_db_user, *arg_dst_db_pasw, *arg_dst_db_read+*arg_dst_db_writ, *arg_dst_schema)
	}
	if *arg_dst_db_driver == "mssql" {
		dbDst, conDst, _ = GetDstMsSqlConnections(*arg_dst_db_host, *arg_dst_db_port, *arg_dst_db_user, *arg_dst_db_pasw, *arg_dst_db_read+*arg_dst_db_writ, *arg_dst_db_name)
	}
	if *arg_dst_db_driver == "postgres" {
		dbDst, conDst, _ = GetDstPostgresConnections(*arg_dst_db_host, *arg_dst_db_port, *arg_dst_db_user, *arg_dst_db_pasw, *arg_dst_db_read+*arg_dst_db_writ, *arg_dst_db_name)
	}
	// ----------------------------------------------------------------------------------
	var tables2sync []aTable
	for _, t := range arg_tables2sync {
		tables2sync = append(tables2sync, aTable{dbName: *arg_schema, tbName: t, dstDbName: *arg_dst_schema})
	}
	if mode_debug {
		log.Print(tables2sync)
	}
	r, _ := GetMetadataInfo4Tables(conSrc[0], tables2sync, *arg_guess_pk, *arg_db_driver, *arg_dst_db_driver)
	if mode_debug {
		log.Printf("tables infos  => %s", r)
	}
	CheckTablesOnDestination(*arg_db_driver, *arg_dst_db_driver, conDst[0], &r)
	// ---------------------------------
	for i := 0; i < len(r); i++ {
		r[i].insert_size = *arg_insert_size
		if *arg_dst_db_driver == "mssql" {
			if r[i].cntCols**arg_insert_size >= 2100 {
				r[i].insert_size = (2100 - 1) / r[i].cntCols
				log.Printf("we change insertsize for % from %s to %d ", r[i].fullName, *arg_insert_size, r[i].insert_size)
			}
		}
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
	// we use chan to communicate between all go subroutines
	//
	tables_to_browse := make(chan int, len(tables2sync)+cntBrowser)
	pk_chunks_to_read := make(chan tablechunk, *arg_db_parr*200)
	chunks_to_read_on_dst := make(chan datachunk, *arg_dst_db_read*200)
	sql_to_write := make(chan insertchunk, *arg_db_parr*400)
	chunk_comparator := make(chan datachunkpair, *arg_db_parr*1000)
	sql_generator := make(chan datarow, *arg_db_parr*3000)
	stat_monitor := make(chan action2Stat, 10000)
	// ------------
	if mode_debug {
		pk_chunks_to_read = make(chan tablechunk, 5)
		chunks_to_read_on_dst = make(chan datachunk, 5)
		sql_to_write = make(chan insertchunk, 5)
		chunk_comparator = make(chan datachunkpair, 5)
		sql_generator = make(chan datarow, 10)
	}
	if mode_trace {
		pk_chunks_to_read = make(chan tablechunk, 1)
		chunks_to_read_on_dst = make(chan datachunk, 1)
		sql_to_write = make(chan insertchunk, 1)
		chunk_comparator = make(chan datachunkpair, 1)
		sql_generator = make(chan datarow, 1)
	}
	// ------------
	for t := 0; t < len(tables2sync); t++ {
		tables_to_browse <- t
	}
	for b := 0; b < cntBrowser; b++ {
		tables_to_browse <- -1
	}
	// ----------------------------------------------------------------------------------
	//
	// browser    : scan tables on db to compute border of each chunk  and pass to a reader
	// reader     : read chunks quickly from the db pass them to dst-reader
	// dst-reader : from a chunk on source , read the same chink on destination , pass both to the comparator
	// comparator : with a pair chunk , compare them , and find difference between source and destination , pass to the generator
	// generator  : from a difference generate a sql code and pass to writer
	// writer     : receive row action , I U D , and inject into a db
	//
	var wg_brw sync.WaitGroup
	var wg_red sync.WaitGroup
	var wg_dst_red sync.WaitGroup
	var wg_cmp sync.WaitGroup
	var wg_gen sync.WaitGroup
	var wg_dst_wrt sync.WaitGroup
	var wg_mon sync.WaitGroup
	// ----------------------------------------------------------------------------------
	wg_mon.Add(1)
	go func() {
		defer wg_mon.Done()
		tableStatsAction(stat_monitor, r, *arg_dumpstats)
	}()
	// ------------
	for j := 0; j < cntBrowser; j++ {
		wg_brw.Add(1)
		go func(adbConn *sql.Conn, id int) {
			defer wg_brw.Done()
			tableChunkBrowser(adbConn, id, tables_to_browse, r, pk_chunks_to_read, *arg_chunk_size, stat_monitor)
		}(conSrc[j], j)
	}
	// ------------
	for j := 0; j < cntReader; j++ {
		time.Sleep(10 * time.Millisecond)
		wg_red.Add(1)
		go func(adbConn *sql.Conn, id int) {
			defer wg_red.Done()
			tableChunkReader(pk_chunks_to_read, chunks_to_read_on_dst, adbConn, r, id, cntBrowser, stat_monitor)
		}(conSrc[j+cntBrowser], j)
	}
	// ------------
	cntComparator := *arg_db_parr * 2
	if mode_debug {
		cntComparator = 2
		if mode_trace {
			cntComparator = 1
		}
	}
	for j := 0; j < cntComparator; j++ {
		wg_cmp.Add(1)
		go func(id int) {
			defer wg_cmp.Done()
			dataChunkComparator(chunk_comparator, id, r, sql_generator, *arg_dst_db_driver, cntBrowser)
		}(j)
	}
	// ------------
	cntGenerator := *arg_db_parr * 2
	if mode_debug {
		cntGenerator = 2
	}
	for j := 0; j < cntGenerator; j++ {
		wg_gen.Add(1)
		go func(id int) {
			defer wg_gen.Done()
			dataSqlGenerator(sql_generator, id, r, sql_to_write, *arg_dst_db_driver, cntBrowser)
		}(j)
	}
	// ------------
	dst_writer_cnt := *arg_dst_db_writ
	for j := 0; j < dst_writer_cnt; j++ {
		wg_dst_wrt.Add(1)
		go func(adbConn *sql.Conn, id int) {
			defer wg_dst_wrt.Done()
			tableDstDbWriter(sql_to_write, adbConn, id+len(conSrc), stat_monitor, *arg_dst_no_insert, *arg_dst_no_update, *arg_dst_no_delete)
		}(conDst[j], j)
	}
	// ------------
	dst_reader_cnt := *arg_dst_db_read
	for j := 0; j < dst_reader_cnt; j++ {
		wg_dst_red.Add(1)
		go func(adbConn *sql.Conn, id int) {
			defer wg_dst_red.Done()
			tableDstChunkReader(chunks_to_read_on_dst, chunk_comparator, adbConn, r, id+len(conSrc)+dst_reader_cnt, cntBrowser, *arg_dst_db_driver, stat_monitor)
		}(conDst[j+dst_writer_cnt], j)
	}
	// ------------
	wg_brw.Wait()
	log.Print("we are done with browser")
	// ------------
	for j := 0; j < cntReader; j++ {
		pk_chunks_to_read <- tablechunk{table_id: -1}
	}
	if mode_debug {
		log.Printf("we added %d empty chunk to flush readers", cntReader)
	}
	wg_red.Wait()
	log.Print("we are done with reader")
	// ------------
	for j := 0; j < dst_reader_cnt; j++ {
		chunks_to_read_on_dst <- datachunk{}
	}
	if mode_debug {
		log.Printf("we added %d empty chunk to flush destination readers", cntReader)
	}
	wg_dst_red.Wait()
	log.Print("we are done with destination reader")
	// ------------
	for j := 0; j < cntComparator; j++ {
		chunk_comparator <- datachunkpair{c_src: nil, c_dst: nil}
	}
	if mode_debug {
		log.Printf("we added %d nil pointer to flush comparators", cntComparator)
	}
	wg_cmp.Wait()
	log.Print("we are done with comparators")
	// ------------
	for j := 0; j < cntGenerator; j++ {
		sql_generator <- datarow{table_id: -1}
	}
	if mode_debug {
		log.Printf("we added %d nil pointer to flush Generator", cntGenerator)
	}
	wg_gen.Wait()
	log.Print("we are done with sql generators")
	// ------------
	for j := 0; j < dst_writer_cnt; j++ {
		sql_to_write <- insertchunk{table_id: -1}
	}
	if mode_debug {
		log.Printf("we added %d nil pointer to flush writers", dst_writer_cnt)
	}
	wg_dst_wrt.Wait()
	log.Print("we are done with writers")
	// ----------------------------------------------------------------------------------
	dbSrc.Close()
	if len(conDst) > 0 {
		dbDst.Close()
	}
	// ----------------------------------------------------------------------------------
	stat_monitor <- action2Stat{TabId: -1}
	wg_mon.Wait()
	// ----------------------------------------------------------------------------------
}

// ------------------------------------------------------------------------------------------
