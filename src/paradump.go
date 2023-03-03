// ------------------------------------------------------------------------------------------
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
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

   ./paradump  -host 127.0.0.1 -db foobar -port 4000 -user foobar -pwd test1234 -table client_activity -table client_info

   ------------------------------------------------------------------------------------------ */

/* ------------------------------------------------------------------------------------------
   https://go.dev/blog/strings

   https://go.dev/blog/slices

   http://go-database-sql.org/importing.html

   https://gobyexample.com/

   https://pkg.go.dev/fmt#Sprintf

   https://pkg.go.dev/database/sql

   https://go.dev/tour/moretypes/15    ( slice )

   https://pkg.go.dev/flag@go1.19.4

   https://www.antoniojgutierrez.com/posts/2021-05-14-short-and-long-options-in-go-flags-pkg/

   https://pkg.go.dev/reflect#DeepEqual

   https://github.com/JamesStewy/go-mysqldump
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
func LockTableWaitRelease(jobsync chan bool, conn *sql.Conn, myPos chan InfoMysqlPosition) {
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
type InfoSqlSession struct {
	Status   bool
	cnxId    int
	Position InfoMysqlPosition
}
type StatSqlSession struct {
	Cnt      int
	FileName string
	FilePos  int
}

func LockTableStartConsistenRead(infoconn chan InfoSqlSession, myId int, conn *sql.Conn, jobstart chan bool, jobsync chan int) {
	var ret_val InfoSqlSession
	ret_val.Status = false
	ret_val.cnxId = myId
	// --------------------
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := conn.PingContext(ctx)
	if p_err != nil {
		log.Fatal("can not ping")
	}
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
	_, e_err := conn.ExecContext(ctx, "SET NAMES utf8mb4")
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
func GetaSynchronizedConnections(DbHost string, DbPort int, DbUsername string, DbUserPassword string, TargetCount int, ConDatabase string) (*sql.DB, []sql.Conn, StatSqlSession, error) {
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
	resultreadchan := make(chan InfoSqlSession, TargetCount*3-1)
	startreadchan := make(chan bool, TargetCount*3-1)
	syncreadchan := make(chan int, TargetCount*3-1)
	// -------------------------------------------
	for i := 1; i < TargetCount*3; i++ {
		go LockTableStartConsistenRead(resultreadchan, i, db_conns[i], startreadchan, syncreadchan)
	}
	// --------------------
	// we wait for TargetCount*3-1  feedback - ready to start a transaction
	for i := 1; i < TargetCount*3; i++ {
		<-syncreadchan
	}
	log.Print("everyone is ready")
	// --------------------
	// we start the read lock
	go LockTableWaitRelease(globallockchan, db_conns[0], globalposchan)
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
	var stats_ses []StatSqlSession
	db_sessions_filepos := make([]InfoSqlSession, TargetCount*3-1)
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
			stats_ses = append(stats_ses, StatSqlSession{Cnt: 1, FileName: db_sessions_filepos[i-1].Position.Name, FilePos: db_sessions_filepos[i-1].Position.Pos})
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
	var ret_dbconns []sql.Conn
	if foundRefPos >= 0 {
		for i := 0; i < TargetCount*3-1; i++ {
			if db_sessions_filepos[i].Position.Name == stats_ses[foundRefPos].FileName && db_sessions_filepos[i].Position.Pos == stats_ses[foundRefPos].FilePos && len(ret_dbconns) < TargetCount {
				ret_dbconns = append(ret_dbconns, *db_conns[db_sessions_filepos[i].cnxId])
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
func GetDstConnections(DbHost string, DbPort int, DbUsername string, DbUserPassword string, TargetCount int, ConDatabase string) (*sql.DB, []sql.Conn, error) {
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
	db_conns := make([]sql.Conn, TargetCount)
	for i := 0; i < TargetCount; i++ {
		first_conn, err := db.Conn(ctx)
		if err != nil {
			log.Print("can not open a mysql connection")
			log.Fatal(err.Error())
		}
		db_conns[i] = *first_conn
		ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
		_, e_err := db_conns[i].ExecContext(ctx, "SET NAMES utf8mb4")
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
type columnInfo struct {
	colName      string
	colType      string
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
	dbName                        string
	tbName                        string
	cntRows                       int64
	storageEng                    string
	cntCols                       int
	cntPkCols                     int
	columnInfos                   []columnInfo
	primaryKey                    []string
	Indexes                       []indexInfo
	fakePrimaryKey                bool
	onError                       int
	fullName                      string
	query_for_reader_interval     string
	param_indices_interval_lo_qry []int
	param_indices_interval_up_qry []int
	query_for_reader_equality     string
	param_indices_equality_qry    []int
	query_for_insert              string
}

// ------------------------------------------------------------------------------------------
func GetTableMetadataInfo(adbConn sql.Conn, dbName string, tableName string, guessPk bool, dumpmode string, dumpinsertwithcol string) (MetadataTable, bool) {
	var result MetadataTable
	result.dbName = dbName
	result.tbName = tableName
	result.fakePrimaryKey = false
	result.onError = 0

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := adbConn.PingContext(ctx)
	if p_err != nil {
		log.Fatalf("can not ping\n%s", p_err.Error())
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err := adbConn.QueryContext(ctx, "select coalesce(TABLE_ROWS,-1),coalesce(ENGINE,'UNKNOW'),TABLE_TYPE from information_schema.tables WHERE table_schema = ? AND table_name = ?     ", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query information_schema.tables for %s.%s\n%s", dbName, tableName, q_err.Error())
	}
	typeTable := "DO_NOT_EXIST"
	for q_rows.Next() {
		err := q_rows.Scan(&result.cntRows, &result.storageEng, &typeTable)
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

	q_rows, q_err = adbConn.QueryContext(ctx, "select COLUMN_NAME , DATA_TYPE,IS_NULLABLE,IFNULL(DATETIME_PRECISION,-9999) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? order by ORDINAL_POSITION ", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query information_schema.columns for %s.%s\n%s", dbName, tableName, q_err.Error())
	}
	for q_rows.Next() {
		var a_col columnInfo
		var a_str string
		var a_int int
		err := q_rows.Scan(&a_col.colName, &a_col.colType, &a_str, &a_int)
		if err != nil {
			log.Print("can not scan columns informations")
			log.Fatal(err.Error())
		}
		a_col.isNullable = (a_str == "YES")
		a_col.isKindChar = (a_col.colType == "char" || a_col.colType == "longtext" || a_col.colType == "mediumtext" || a_col.colType == "text" || a_col.colType == "tinytext" || a_col.colType == "varchar")
		a_col.isKindBinary = (a_col.colType == "varbinary" || a_col.colType == "binary" || a_col.colType == "tinyblob" || a_col.colType == "blob" || a_col.colType == "longblob" || a_col.colType == "bit")
		a_col.mustBeQuote = a_col.isKindChar || a_col.isKindBinary || (a_col.colType == "date" || a_col.colType == "datetime" || a_col.colType == "time" || a_col.colType == "timestamp")
		a_col.isKindFloat = (a_col.colType == "float" || a_col.colType == "double")
		a_col.haveFract = (a_col.colType == "datetime" || a_col.colType == "timestamp" || a_col.colType == "time") && (a_int > 0)
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

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err = adbConn.QueryContext(ctx, "select COLUMN_NAME,coalesce(CARDINALITY,0),INDEX_NAME  from INFORMATION_SCHEMA.STATISTICS WHERE  table_schema = ? and table_name = ? and INDEX_NAME != 'PRIMARY' order by INDEX_NAME,SEQ_IN_INDEX", dbName, tableName)
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
	// ---------------------------------------------------------------------------------
	result.fullName = fmt.Sprintf("`%s`.`%s`", result.dbName, result.tbName)

	sql_cond_lower_pk, qry_indices_lo_bound := generatePredicat(result.primaryKey, true)
	sql_cond_upper_pk, qry_indices_up_bound := generatePredicat(result.primaryKey, false)
	sql_cond_equal_pk, qry_indices_equality := generateEqualityPredicat(result.primaryKey)
	sql_tab_cols := generateListCols4Sql(result.columnInfos)
	result.query_for_reader_equality = fmt.Sprintf("/* paradump */ select %s from %s where ( %s )           ", sql_tab_cols, result.fullName, sql_cond_equal_pk)
	result.param_indices_equality_qry = qry_indices_equality
	result.query_for_reader_interval = fmt.Sprintf("/* paradump */ select %s from %s where ( %s ) and ( %s) ", sql_tab_cols, result.fullName, sql_cond_lower_pk, sql_cond_upper_pk)
	result.param_indices_interval_lo_qry = qry_indices_lo_bound
	result.param_indices_interval_up_qry = qry_indices_up_bound

	result.cntCols = len(result.columnInfos)
	result.cntPkCols = len(result.primaryKey)
	// ---------------------------------------------------------------------------------
	if dumpmode == "cpy" {
		result.query_for_insert = fmt.Sprintf("INSERT INTO %s(%s) VALUES (", result.fullName, sql_tab_cols)
	} else {
		if dumpinsertwithcol == "full" {
			result.query_for_insert = fmt.Sprintf("INSERT INTO `%s`(%s) VALUES (", result.tbName, sql_tab_cols)
		} else {
			result.query_for_insert = fmt.Sprintf("INSERT INTO `%s` VALUES (", result.tbName)
		}
	}
	// ---------------------------------------------------------------------------------
	return result, true
}

// ------------------------------------------------------------------------------------------
func GetListTables(adbConn sql.Conn, dbNames []string, tab2exclude []string) []aTable {
	var result []aTable
	for _, v := range dbNames {
		result = append(result, GetListTablesBySchema(adbConn, v, tab2exclude)[:]...)
	}
	return result
}

// ------------------------------------------------------------------------------------------
func GetListTablesBySchema(adbConn sql.Conn, dbName string, tab2exclude []string) []aTable {
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
func GetMetadataInfo4Tables(adbConn sql.Conn, tableNames []aTable, guessPk bool, dumpmode string, dumpinsertwithcol string) ([]MetadataTable, bool) {
	var result []MetadataTable
	for j := 0; j < len(tableNames); j++ {
		info, _ := GetTableMetadataInfo(adbConn, tableNames[j].dbName, tableNames[j].tbName, guessPk, dumpmode, dumpinsertwithcol)
		result = append(result, info)
	}
	log.Printf("-------------------")
	cnt := 0
	for _, v := range result {
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
	}
	if cnt > 0 {
		log.Fatalf("too many ERRORS")
	}
	return result, true
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
	cols []colchunk
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
func generatePredicat(pkeyCols []string, lowerbound bool) (string, []int) {
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
		if ncolpkey == i {
			sql_pred = sql_pred + fmt.Sprintf(" ( `%s` %s ? ) ", pkeyCols[i], op_1)
		} else {
			sql_pred = sql_pred + fmt.Sprintf(" ( `%s` %s ? ) ", pkeyCols[i], op_o)
		}
		sql_vals_indices = append(sql_vals_indices, i)
		if ncolpkey > 0 {
			for j := 0; j < ncolpkey; j++ {
				if j < i {
					sql_pred = sql_pred + fmt.Sprintf(" and ( `%s` = ? ) ", pkeyCols[j])
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
func generateEqualityPredicat(pkeyCols []string) (string, []int) {
	var sql_pred string
	sql_vals_indices := make([]int, 0)
	sql_pred = " ( "
	for i := range pkeyCols {
		if i != 0 {
			sql_pred = sql_pred + " and "
		}
		sql_pred = sql_pred + fmt.Sprintf(" ( `%s` = ? ) ", pkeyCols[i])
		sql_vals_indices = append(sql_vals_indices, i)
	}
	sql_pred = sql_pred + " ) "
	return sql_pred, sql_vals_indices
}

// ------------------------------------------------------------------------------------------
func tableChunkBrowser(adbConn sql.Conn, id int, tableidstoscan chan int, tableInfos []MetadataTable, chunk2read chan tablechunk, sizeofchunk_init int64) {
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
		sql_lst_pk_cols := fmt.Sprintf("`%s`", tableInfos[j].primaryKey[0])
		c := 1
		for c < len(tableInfos[j].primaryKey) {
			sql_lst_pk_cols = sql_lst_pk_cols + "," + fmt.Sprintf("`%s`", tableInfos[j].primaryKey[c])
			c++
		}
		sql_full_tab_name := fmt.Sprintf("`%s`.`%s`", tableInfos[j].dbName, tableInfos[j].tbName)
		the_query := fmt.Sprintf("select %s from %s order by %s limit 1 ", sql_lst_pk_cols, sql_full_tab_name, sql_lst_pk_cols)
		if mode_debug {
			log.Printf("table %s size pk %d query :  %s \n", sql_full_tab_name, c, the_query)
		}
		ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
		q_rows, q_err := adbConn.QueryContext(ctx, the_query)
		if q_err != nil {
			log.Fatalf("can not query table %s to get first pk aka ( %s )\n%s", sql_full_tab_name, sql_lst_pk_cols, q_err.Error())
		}
		a_sql_row := make([]*sql.NullString, c)
		var pk_cnt int64
		ptrs := make([]any, c)
		var ptrs_nextpk []any
		if tableInfos[j].fakePrimaryKey {
			ptrs_nextpk = make([]any, c+1)
		} else {
			ptrs_nextpk = make([]any, c)
		}
		for i := range a_sql_row {
			ptrs[i] = &a_sql_row[i]
			ptrs_nextpk[i] = &a_sql_row[i]
		}
		if tableInfos[j].fakePrimaryKey {
			ptrs_nextpk[c] = &pk_cnt
		} else {
			pk_cnt = -1
		}
		row_cnt := 0
		for q_rows.Next() {
			err := q_rows.Scan(ptrs...)
			if err != nil {
				log.Printf("can not scan result for table %s \n", sql_full_tab_name)
				log.Fatal(err.Error())
			}
			row_cnt++
		}
		if row_cnt == 0 {
			log.Printf("table["+format_cnt_table+"] %s is empty \n", j, sql_full_tab_name)
			continue
		}
		start_pk_row := make([]string, c)
		for n, value := range a_sql_row {
			start_pk_row[n] = value.String
		}
		log.Printf("table["+format_cnt_table+"] %s first pk ( %s ) - start scan pk %s\n", j, sql_full_tab_name, sql_lst_pk_cols, start_pk_row)
		// --------------------------------------------------------------------------
		sql_cond_pk, sql_val_indices_pk := generatePredicat(tableInfos[j].primaryKey, true)
		var sql_order_pk_cols string
		for i, v := range tableInfos[j].primaryKey {
			if i != 0 {
				sql_order_pk_cols = sql_order_pk_cols + ","
			}
			sql_order_pk_cols = sql_order_pk_cols + fmt.Sprintf("`%s` desc ", v)
		}
		var the_finish_query string
		var prepare_finish_query *sql.Stmt
		var p_err error
		var sql_vals_pk []any
		var end_pk_row []string
		for {
			if tableInfos[j].fakePrimaryKey {
				the_finish_query = fmt.Sprintf("select %s,cast(@cnt as unsigned ) as _cnt_pkey from ( select %s,@cnt:=@cnt+1 from %s , ( select @cnt := 0 ) c where %s order by %s limit %d ) e order by %s limit 1 ",
					sql_lst_pk_cols, sql_lst_pk_cols, sql_full_tab_name, sql_cond_pk, sql_lst_pk_cols, sizeofchunk, sql_order_pk_cols)
			} else {
				the_finish_query = fmt.Sprintf("select %s                                      from ( select %s              from %s                          where %s order by %s limit %d ) e order by %s limit 1 ",
					sql_lst_pk_cols, sql_lst_pk_cols, sql_full_tab_name, sql_cond_pk, sql_lst_pk_cols, sizeofchunk, sql_order_pk_cols)
			}
			ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
			prepare_finish_query, p_err = adbConn.PrepareContext(ctx, the_finish_query)
			if p_err != nil {
				log.Fatalf("can not prepare to get next pk ( %s )\n%s", the_finish_query, p_err.Error())
			}
			sql_vals_pk = generateValuesForPredicat(sql_val_indices_pk, start_pk_row)
			if mode_debug {
				log.Printf("table %s query :  %s with %d params sizeofchunk %d\n", sql_full_tab_name, the_finish_query, len(sql_vals_pk), sizeofchunk)
			}
			q_rows, q_err = prepare_finish_query.Query(sql_vals_pk...)
			if q_err != nil {
				log.Fatalf("can not query table %s to get next pk aka ( %s )\n%s", sql_full_tab_name, sql_lst_pk_cols, q_err.Error())
			}
			for q_rows.Next() {
				err := q_rows.Scan(ptrs_nextpk...)
				if err != nil {
					log.Printf("can not scan result for table %s doing get next pk ( len ptrs_nextpk %d ) \n", sql_full_tab_name, len(ptrs_nextpk))
					log.Fatal(err.Error())
				}
			}
			end_pk_row = make([]string, c)
			for n, value := range a_sql_row {
				end_pk_row[n] = value.String
			}
			if !tableInfos[j].fakePrimaryKey || !reflect.DeepEqual(start_pk_row, end_pk_row) || pk_cnt < sizeofchunk {
				break
			} else {
				log.Printf("we change sizeofchunk from %d to %d", sizeofchunk, sizeofchunk*15/10)
				sizeofchunk = sizeofchunk * 15 / 10
				_ = prepare_finish_query.Close()
			}
		}
		if mode_debug {
			log.Printf("table %s end pk ( %s ) scan pk %d : %s sizeofchunk: %d \n", sql_full_tab_name, sql_lst_pk_cols, pk_cnt, end_pk_row, sizeofchunk)
		}
		// ------------------------------------------------------------------
		var chunk_id int64 = 100000000 * (int64(j) + 1)
		if mode_debug {
			log.Printf("%s\n", reflect.DeepEqual(start_pk_row, end_pk_row))
		}
		begin_equal_end := reflect.DeepEqual(start_pk_row, end_pk_row)
		for !begin_equal_end {
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
			if sizeofchunk > sizeofchunk_init {
				must_prepare_query = true
				sizeofchunk = sizeofchunk_init
			}
			// ------------------------------------------------------------------
			for {
				// ----------------------------------------------------------
				start_pk_row = end_pk_row
				// ----------------------------------------------------------
				if must_prepare_query {
					the_finish_query = fmt.Sprintf("select %s,cast(@cnt as unsigned ) as _cnt_pkey from ( select %s,@cnt:=@cnt+1 from %s , ( select @cnt := 0 ) c where %s order by %s limit %d ) e order by %s limit 1 ",
						sql_lst_pk_cols, sql_lst_pk_cols, sql_full_tab_name, sql_cond_pk, sql_lst_pk_cols, sizeofchunk, sql_order_pk_cols)
					_ = prepare_finish_query.Close()
					ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
					prepare_finish_query, p_err = adbConn.PrepareContext(ctx, the_finish_query)
					if p_err != nil {
						log.Fatalf("can not prepare to get next pk ( %s )\n%s", the_finish_query, p_err.Error())
					}
					must_prepare_query = false
				}
				// ----------------------------------------------------------
				sql_vals_pk := generateValuesForPredicat(sql_val_indices_pk, start_pk_row)
				q_rows, q_err = prepare_finish_query.Query(sql_vals_pk...)
				if q_err != nil {
					log.Fatalf("can not query table %s to get next pk aka ( %s )\n%s", sql_full_tab_name, sql_lst_pk_cols, q_err.Error())
				}
				for q_rows.Next() {
					err := q_rows.Scan(ptrs_nextpk...)
					if err != nil {
						log.Printf("can not scan result for table %s doing get next pk ( len ptrs_nextpk %d ) ( len sql_vals_pk %d) \n", sql_full_tab_name, len(ptrs_nextpk), len(sql_vals_pk))
						log.Printf("query for nextpk is %s", the_finish_query)
						log.Fatal(err.Error())
					}
				}
				end_pk_row = make([]string, c)
				for n, value := range a_sql_row {
					end_pk_row[n] = value.String
				}
				if mode_debug {
					log.Printf("table %s query :  %s \n with %d params\nval for query: %s\nresult: %s\n", sql_full_tab_name, the_finish_query, len(sql_vals_pk), sql_vals_pk, end_pk_row)
					log.Printf("table %s pk interval : %s -> %s\n", sql_full_tab_name, start_pk_row, end_pk_row)
					log.Printf("%s\n", reflect.DeepEqual(start_pk_row, end_pk_row))
				}
				begin_equal_end = reflect.DeepEqual(start_pk_row, end_pk_row)
				if !tableInfos[j].fakePrimaryKey || !begin_equal_end || pk_cnt < sizeofchunk {
					break
				} else {
					sizeofchunk = sizeofchunk * 15 / 10
					must_prepare_query = true
				}
				// ----------------------------------------------------------
			}
			if mode_debug {
				log.Printf("table %s query :  %s \n with %d params\nval for query: %s\nresult: %s\n", sql_full_tab_name, the_finish_query, len(sql_vals_pk), sql_vals_pk, end_pk_row)
				log.Printf("table %s pk interval : %s -> %s\n", sql_full_tab_name, start_pk_row, end_pk_row)
				log.Printf("%s\n", reflect.DeepEqual(start_pk_row, end_pk_row))
			}
			// ------------------------------------------------------------------
		}
		log.Printf("table["+format_cnt_table+"] %s scan is done , pk col ( %s ) scan size pk %d last pk %s\n", j, sql_full_tab_name, sql_lst_pk_cols, pk_cnt, end_pk_row)
		// --------------------------------------------------------------------------
		chunk_id++
		var a_chunk tablechunk
		a_chunk.table_id = j
		a_chunk.chunk_id = chunk_id
		a_chunk.begin_val = start_pk_row
		a_chunk.end_val = end_pk_row
		a_chunk.begin_equal_end = begin_equal_end
		a_chunk.is_done = false
		chunk2read <- a_chunk
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
func ChunkReaderDumpProcess(threadid int, q_rows *sql.Rows, a_sql_row *[]sql.NullString, a_table_info *MetadataTable, tab_id int, chk_id int64, ptrs *[]any, insert_size int, chan2gen chan datachunk) {
	// --------------------------------------------------------------------------
	var a_dta_chunk datachunk
	a_dta_chunk = datachunk{usedlen: 0, chunk_id: chk_id, table_id: tab_id, rows: make([]*rowchunk, insert_size)}
	row_cnt := 0
	if mode_debug {
		log.Printf("ChunkReaderDumpProcess [%02d] table %03d chunk %12d \n", threadid, tab_id, chk_id)
	}
	for q_rows.Next() {
		err := q_rows.Scan(*ptrs...)
		if err != nil {
			log.Printf("can not scan %s ( already scan %d ) , len(ptrs) = %d , cols %s ", a_table_info.tbName, row_cnt, len(*ptrs))
			log.Fatal(err)
		}
		var a_simple_row rowchunk
		a_simple_row.cols = make([]colchunk, a_table_info.cntCols)
		// ------------------------------------------------------------------
		for n, value := range *a_sql_row {
			if !value.Valid {
				a_simple_row.cols[n].kind = -1
			} else {
				a_simple_row.cols[n].kind = 999
				a_str := value.String
				a_simple_row.cols[n].val = &a_str
			}
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
	a_sql_row              []sql.NullString
	ptrs                   []any
}

// ------------------------------------------------------------------------------------------
func tableChunkReader(chunk2read chan tablechunk, chan2generator chan datachunk, adbConn sql.Conn, tableInfos []MetadataTable, id int, insert_size int, cntBrowser int) {
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
			for n, value := range tabReadingVars {
				if value == nil {
					empty_slot = n
				} else if value.table_id == a_chunk.table_id {
					tab_found = n
					break
				} else {
					if value.lastusagecnt < lru_val {
						lru_val = value.lastusagecnt
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
				new_info.a_sql_row = make([]sql.NullString, tableInfos[a_chunk.table_id].cntCols)
				new_info.ptrs = make([]any, tableInfos[a_chunk.table_id].cntCols)
				for i := 0; i < tableInfos[a_chunk.table_id].cntCols; i++ {
					new_info.ptrs[i] = &new_info.a_sql_row[i]
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
		ChunkReaderDumpProcess(id, q_rows, &last_table.a_sql_row, &tableInfos[last_table.table_id], last_table.table_id, a_chunk.chunk_id, &last_table.ptrs, insert_size, chan2generator)
		// --------------------------------------------------------------------------
		if mode_debug {
			log.Printf("table %s chunk query :  %s \n with %d params\nval for query: %s\n", last_table.fullname, *the_query, len(sql_vals_pk), sql_vals_pk)
		}
		// --------------------------------------------------------------------------
	}
	// ----------------------------------------------------------------------------------
	for _, value := range tabReadingVars {
		if value != nil {
			value.interval_prepared_stmt.Close()
			value.equality_prepared_stmt.Close()
		}
	}
	// ----------------------------------------------------------------------------------
	if mode_debug {
		log.Printf("tableChunkReader[%02d] finish cntreadchunk:%9d last_hit: %9d cache_hit: %9d cache_miss:%9d\n", id, cntreadchunk, last_hit, cache_hit, cache_miss)
		log.Printf("tableChunkReader[%02d] finish\n", id)
	}
}

// ------------------------------------------------------------------------------------------
func generateListCols4Sql(col_inf []columnInfo) string {
	a_str := "`" + col_inf[0].colName + "`"
	ctab := 1
	for ctab < len(col_inf) {
		a_str = a_str + ",`" + col_inf[ctab].colName + "`"
		ctab++
	}
	return a_str
}

// ------------------------------------------------------------------------------------------
func dataChunkGenerator(rowvalueschan chan datachunk, id int, tableInfos []MetadataTable, dumpmode string, sql2inject chan insertchunk, insert_size int) {
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

		var insert_sql_arr []*string
		var b_siz_init int
		var last_table_id int = -1
		var tab_meta *MetadataTable
		// ------------------------------------------------------------------
		a_dta_chunk := <-rowvalueschan
		for {
			if mode_debug {
				log.Printf("[%02d] dataChunkGenerator table %03d chunk %12d usedlen %5d", id, a_dta_chunk.table_id, a_dta_chunk.chunk_id, a_dta_chunk.usedlen)
			}
			if a_dta_chunk.table_id == -1 {
				break
			}
			if last_table_id != a_dta_chunk.table_id {
				last_table_id = a_dta_chunk.table_id
				tab_meta = &tableInfos[last_table_id]
				// -------------------------------------------------
				insert_sql_arr = make([]*string, insert_size*2*tab_meta.cntCols+1)
				// -------------------------------------------------
				u_ind := 0
				coma_str := ","
				for r := 0; r < insert_size; r++ {
					insert_sql_arr[u_ind] = &betwStr
					u_ind += 2
					for c := 1; c < tab_meta.cntCols; c++ {
						insert_sql_arr[u_ind] = &coma_str
						u_ind += 2
					}
				}
				insert_sql_arr[0] = &tab_meta.query_for_insert
				insert_sql_arr[u_ind] = &endStr
				// -------------------------------------------------
				b_siz_init = len(tab_meta.query_for_insert)
			}
			arr_ind := 1
			b_siz := b_siz_init + a_dta_chunk.usedlen*(3+(tab_meta.cntCols-1))

			for j := 0; j < a_dta_chunk.usedlen; j++ {
				// -------------------------------------------------
				for n, value := range a_dta_chunk.rows[j].cols {
					if value.kind == -1 {
						insert_sql_arr[arr_ind] = &nullStr
					} else {
						if tab_meta.columnInfos[n].mustBeQuote {
							if strings.ContainsAny(*value.val, "\\\u0000\u001a\n\r\"'") {
								v := strings.ReplaceAll(*value.val, "\\", "\\\\")
								v = strings.ReplaceAll(v, "\u0000", "\\0")
								v = strings.ReplaceAll(v, "\u001a", "\\Z")
								v = strings.ReplaceAll(v, "\n", "\\n")
								v = strings.ReplaceAll(v, "\r", "\\r")
								v = strings.ReplaceAll(v, "'", "\\'")
								v = strings.ReplaceAll(v, "\"", "\\\"")
								v = "'" + v + "'"
								insert_sql_arr[arr_ind] = &v
							} else {
								v := "'" + *value.val + "'"
								insert_sql_arr[arr_ind] = &v
							}
						} else {
							if tab_meta.columnInfos[n].isKindFloat {
								var f_prec uint = 24
								if tab_meta.columnInfos[n].colType == "double" {
									f_prec = 53
								}
								f := new(big.Float).SetPrec(f_prec)
								f.SetString(*value.val)
								v := f.Text('f', -1)
								insert_sql_arr[arr_ind] = &v
							} else {
								insert_sql_arr[arr_ind] = value.val
							}
						}
					}
					b_siz += len(*insert_sql_arr[arr_ind])
					arr_ind += 2
				}
				// --------------------------------------------------
			}
			// ----------------------------------------------------------
			if arr_ind < 2*insert_size*tab_meta.cntCols {
				insert_sql_arr[arr_ind-1] = &endStr
			}
			var b strings.Builder
			b.Grow(b_siz)
			for _, s := range insert_sql_arr[:a_dta_chunk.usedlen*2*tab_meta.cntCols+1] {
				b.WriteString(*s)
			}
			a_str := b.String()
			if mode_trace {
				log.Printf("%s", a_str)
			}
			if arr_ind < 2*insert_size*tab_meta.cntCols {
				insert_sql_arr[arr_ind-1] = &betwStr
			}
			if mode_trace {
				log.Printf("%s", a_str)
			}
			// ----------------------------------------------------------
			if mode_debug {
				log.Printf("[%02d] dataChunkGenerator table %03d chunk %12d ... sql len is %6d", id, a_dta_chunk.table_id, a_dta_chunk.chunk_id, len(a_str))
			}
			sql2inject <- insertchunk{table_id: last_table_id, chunk_id: a_dta_chunk.chunk_id, sql: &a_str}
			if mode_debug {
				log.Printf("[%02d] dataChunkGenerator table %03d chunk %12d ... ok sql2inject", id, a_dta_chunk.table_id, a_dta_chunk.chunk_id)
			}
			// ----------------------------------------------------------
			a_dta_chunk = <-rowvalueschan
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
			b_ind := 0
			b_siz := a_dta_chunk.usedlen * tab_meta.cntCols
			for j := 0; j < a_dta_chunk.usedlen; j++ {
				// -------------------------------------------------
				for n, value := range a_dta_chunk.rows[j].cols {
					if value.kind == -1 {
						if tab_meta.columnInfos[n].isKindChar {
							buf_arr[b_ind] = &nullStr
						} else {
							buf_arr[b_ind] = &emptStr
						}
					} else {
						if tab_meta.columnInfos[n].mustBeQuote && strings.ContainsAny(*value.val, "\n,\"") {
							a_str := "'" + strings.ReplaceAll(*value.val, "\"", "\"\"") + "'"
							buf_arr[b_ind] = &a_str
						} else if tab_meta.columnInfos[n].haveFract {
							timeSec, timeFract, dotFound := strings.Cut(*value.val, ".")
							if dotFound {
								timeFract = strings.TrimRight(timeFract, "0")
								if len(timeFract) == 1 {
									timeFract = timeFract + "0"
								}
								a_str := timeSec + "." + timeFract
								buf_arr[b_ind] = &a_str
							} else {
								buf_arr[b_ind] = value.val
							}
						} else {
							buf_arr[b_ind] = value.val
						}
					}
					b_siz += len(*buf_arr[b_ind])
					b_ind += 2
				}
				// -------------------------------------------------
			}
			var b strings.Builder
			b.Grow(b_siz)
			for _, s := range buf_arr[:a_dta_chunk.usedlen*2*tab_meta.cntCols] {
				b.WriteString(*s)
			}
			j_str := b.String()
			sql2inject <- insertchunk{table_id: last_table_id, sql: &j_str}
			// ----------------------------------------------------------
			a_dta_chunk = <-rowvalueschan
		}
	}
	// ----------------------------------------------------------------------------------
}

// ------------------------------------------------------------------------------------------
func tableFileWriter(sql2inject chan insertchunk, id int, tableInfos []MetadataTable, dumpfiletemplate string, dumpmode string, dumpheader bool, dumpcompress string, z_level int, z_para int) {
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
		if dumpcompress == "zstd" {
			fname = strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(dumpfiletemplate+"."+dumpcompress, "%d", v.dbName), "%t", v.tbName), "%p", strconv.Itoa(id)), "%m", dumpmode)
		} else {
			fname = strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(dumpfiletemplate, "%d", v.dbName), "%t", v.tbName), "%p", strconv.Itoa(id)), "%m", dumpmode)
		}
		fh, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			log.Printf("can not openfile %s", fname)
			log.Fatal(err.Error())
		} else {
			fh.Close()
		}
		file_is_empty = append(file_is_empty, true)
		file_name = append(file_name, fname)
	}
	// ----------------------------------------------------------------------------------
	var und_fh *os.File
	var err error
	last_tableid := -1
	a_insert_sql := <-sql2inject
	// ----------------------------
	if dumpcompress == "zstd" {
		// --------------------------------------------------------------------------
		var zst_enc *zstd.Encoder
		// --------------------------------------------------------------------------
		for a_insert_sql.sql != nil {
			if mode_debug {
				log.Printf("[%02d] tableFileWriter table %03d chunk %12d sql len %6d", id, a_insert_sql.table_id, a_insert_sql.chunk_id, len(*a_insert_sql.sql))
			}
			// ------------------------------------------------------------------
			if last_tableid != a_insert_sql.table_id {
				last_tableid = a_insert_sql.table_id
				if und_fh != nil {
					if zst_enc != nil {
						zst_enc.Close()
					}
					und_fh.Close()
				}
				fname := file_name[last_tableid]
				und_fh, err = os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
				if err != nil {
					log.Printf("can not openfile %s", fname)
					log.Fatal(err.Error())
				}
				zst_enc, err = zstd.NewWriter(und_fh, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(z_level)), zstd.WithEncoderConcurrency(z_para))
				if err != nil {
					log.Printf("can not create zstd.NewWriter")
					log.Fatal(err.Error())
				}
				if file_is_empty[last_tableid] {
					if dumpheader {
						var n_pat string
						if dumpmode == "csv" {
							n_pat = "%s"
						} else {
							n_pat = "`%s`"
						}
						sql_tab_cols := fmt.Sprintf(n_pat, tableInfos[last_tableid].columnInfos[0].colName)
						ctab := 1
						for ctab < len(tableInfos[last_tableid].columnInfos) {
							sql_tab_cols = sql_tab_cols + "," + fmt.Sprintf(n_pat, tableInfos[last_tableid].columnInfos[ctab].colName)
							ctab++
						}
						ChunkReaderDumpHeader(dumpmode, zst_enc, sql_tab_cols)
					}
					file_is_empty[last_tableid] = false
				}
			}
			// ------------------------------------------------------------------
			io.WriteString(zst_enc, *a_insert_sql.sql)
			// ------------------------------------------------------------------
			a_insert_sql = <-sql2inject
		}
		// --------------------------------------------------------------------------
		if zst_enc != nil {
			zst_enc.Close()
		}
	} else {
		// --------------------------------------------------------------------------
		for a_insert_sql.sql != nil {
			if mode_debug {
				log.Printf("[%02d] tableFileWriter table %03d chunk %12d sql len %6d", id, a_insert_sql.table_id, a_insert_sql.chunk_id, len(*a_insert_sql.sql))
			}
			// ------------------------------------------------------------------
			if last_tableid != a_insert_sql.table_id {
				last_tableid = a_insert_sql.table_id
				if und_fh != nil {
					und_fh.Close()
				}
				fname := file_name[last_tableid]
				und_fh, err = os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
				if err != nil {
					log.Printf("can not openfile %s", fname)
					log.Fatal(err.Error())
				}
				if file_is_empty[last_tableid] {
					if dumpheader {
						var n_pat string
						if dumpmode == "csv" {
							n_pat = "%s"
						} else {
							n_pat = "`%s`"
						}
						sql_tab_cols := fmt.Sprintf(n_pat, tableInfos[last_tableid].columnInfos[0].colName)
						ctab := 1
						for ctab < len(tableInfos[last_tableid].columnInfos) {
							sql_tab_cols = sql_tab_cols + "," + fmt.Sprintf(n_pat, tableInfos[last_tableid].columnInfos[ctab].colName)
							ctab++
						}
						ChunkReaderDumpHeader(dumpmode, und_fh, sql_tab_cols)
					}
					file_is_empty[last_tableid] = false
				}
			}
			// ------------------------------------------------------------------
			io.WriteString(und_fh, *a_insert_sql.sql)
			// ------------------------------------------------------------------
			if mode_debug {
				log.Printf("[%02d] tableFileWriter table %03d chunk %12d wait next", id, a_insert_sql.table_id, a_insert_sql.chunk_id)
			}
			a_insert_sql = <-sql2inject
		}
		// --------------------------------------------------------------------------
	}
	if und_fh != nil {
		und_fh.Close()
	}
	// ----------------------------------------------------------------------------------
	if mode_debug {
		log.Printf("tableFileWriter[%d] finish\n", id)
	}
}

// ------------------------------------------------------------------------------------------
func tableCopyWriter(sql2inject chan insertchunk, adbConn sql.Conn, id int) {
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
	arg_debug := flag.Bool("debug", false, "debug mode")
	arg_trace := flag.Bool("trace", false, "trace mode")
	arg_loop := flag.Int("loopcnt", 1, "how many times we are going to loop (for debugging)")
	// ----------------------------------------------------------------------------------
	arg_db_port := flag.Int("port", 3306, "the database port")
	arg_db_host := flag.String("host", "127.0.0.1", "the database host")
	arg_db_user := flag.String("user", "mysql", "the database connection user")
	arg_db_pasw := flag.String("pwd", "", "the database connection password")
	arg_browser_parr := flag.Int("browser", 4, "number of browsers")
	arg_db_parr := flag.Int("parallel", 10, "number of workers")
	arg_chunk_size := flag.Int("chunksize", 10000, "rows count when reading")
	arg_insert_size := flag.Int("insertsize", 500, "rows count for each insert")
	arg_dumpfile := flag.String("dumpfile", "dump_%d_%t_%p.%m", "sql dump of tables")
	arg_dumpmode := flag.String("dumpmode", "sql", "format of the dump , csv / sql / cpy ")
	arg_dumpheader := flag.Bool("dumpheader", true, "add a header on csv/sql")
	arg_dumpinsert := flag.String("dumpinsert", "full", "specify column names on insert , full /simple ")
	arg_dumpparr := flag.Int("dumpparallel", 5, "number of file writers")
	arg_dumpcompress := flag.String("dumpcompress", "", "which compression format to use , zstd")
	arg_dumpcompress_level := flag.Int("dumpcompresslevel", 1, "which zstd compression level ( 1 , 3 , 6 , 11 ) ")
	arg_dumpcompress_concur := flag.Int("dumpcompressconcur", 4, "which zstd compression concurency ")
	// ------------
	var arg_dbs arrayFlags
	flag.Var(&arg_dbs, "db", "database(s) of tables to dump")
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
	arg_dst_db_port := flag.Int("dst-port", 3306, "the database port")
	arg_dst_db_host := flag.String("dst-host", "127.0.0.1", "the database host")
	arg_dst_db_user := flag.String("dst-user", "mysql", "the database connection user")
	arg_dst_db_pasw := flag.String("dst-pwd", "", "the database connection password")
	arg_dst_db_parr := flag.Int("dst-parallel", 20, "number of workers")
	// ------------
	flag.Parse()
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
	if arg_dbs == nil {
		log.Printf("no database specified")
		flag.Usage()
		os.Exit(4)
	}
	if len(arg_dbs) > 1 && arg_tables2dump != nil {
		log.Printf("can not specify multiple databases with a list of tables")
		flag.Usage()
		os.Exit(5)
	}
	if len(*arg_dumpfile) == 0 && *arg_dumpmode != "cpy" {
		flag.Usage()
		os.Exit(6)
	}
	if len(*arg_dumpmode) != 0 && (*arg_dumpmode != "sql" && *arg_dumpmode != "csv" && *arg_dumpmode != "cpy") {
		flag.Usage()
		os.Exit(7)
	}
	if len(*arg_dumpcompress) != 0 && (*arg_dumpcompress != "zstd") {
		flag.Usage()
		os.Exit(8)
	}
	if *arg_insert_size > *arg_chunk_size {
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
	if len(flag.Args()) > 0 {
		log.Printf("extra arguments ( non-recognized) on command line")
		for _, v := range flag.Args() {
			log.Printf(" '%s'    is not recognized \n", v)
		}
		flag.Usage()
		os.Exit(11)
	}
	if arg_tables2dump != nil && len(arg_tables2exclude) != 0 {
		log.Printf("can not specify table to exclude with a list of tables")
		flag.Usage()
		os.Exit(12)
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
	var cntBrowser int = *arg_browser_parr
	var cntReader int = *arg_db_parr
	// ----------------------------------------------------------------------------------
	var conSrc []sql.Conn
	var conDst []sql.Conn
	var dbSrc *sql.DB
	var dbDst *sql.DB
	dbSrc, conSrc, _, _ = GetaSynchronizedConnections(*arg_db_host, *arg_db_port, *arg_db_user, *arg_db_pasw, cntReader+cntBrowser, arg_dbs[0])
	if *arg_dumpmode == "cpy" {
		dbDst, conDst, _ = GetDstConnections(*arg_dst_db_host, *arg_dst_db_port, *arg_dst_db_user, *arg_dst_db_pasw, *arg_dst_db_parr, arg_dbs[0])
	}
	// ----------------------------------------------------------------------------------
	var tables2dump []aTable
	if arg_tables2dump == nil {
		tables2dump = GetListTables(conSrc[0], arg_dbs, arg_tables2exclude)
	} else {
		for _, t := range arg_tables2dump {
			tables2dump = append(tables2dump, aTable{dbName: arg_dbs[0], tbName: t})
		}
	}
	if mode_debug {
		log.Print(tables2dump)
	}
	r, _ := GetMetadataInfo4Tables(conSrc[0], tables2dump, *arg_guess_pk, *arg_dumpmode, *arg_dumpinsert)
	if mode_debug {
		log.Printf("tables infos  => %s", r)
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
		go func(adbConn sql.Conn, id int) {
			defer wg_brw.Done()
			tableChunkBrowser(adbConn, id, tables_to_browse, r, pk_chunks_to_read, int64(*arg_chunk_size))
		}(conSrc[j], j)
	}
	// ------------
	for j := 0; j < cntReader; j++ {
		time.Sleep(10 * time.Millisecond)
		wg_red.Add(1)
		go func(adbConn sql.Conn, id int) {
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
			dataChunkGenerator(sql_generator, id, r, *arg_dumpmode, sql_to_write, *arg_insert_size)
		}(j)
	}
	// ------------
	writer_cnt := 0
	if len(conDst) > 0 {
		writer_cnt = len(conDst)
		for j := 0; j < writer_cnt; j++ {
			wg_wrt.Add(1)
			go func(adbConn sql.Conn, id int) {
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
				tableFileWriter(sql_to_write, id, r, *arg_dumpfile, *arg_dumpmode, *arg_dumpheader, *arg_dumpcompress, zstd_level, zstd_concur)
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
