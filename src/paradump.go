// ------------------------------------------------------------------------------------------
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
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
var mode_debug bool

// ------------------------------------------------------------------------------------------
type InfoMysqlPosition struct {
	Name       string
	Pos        int
}
// ------------------------------------------------------------------------------------------
func LockTableWaitRelease(jobsync chan bool, conn *sql.Conn , myPos chan InfoMysqlPosition ) {
	var ret_val InfoMysqlPosition
	// --------------------
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := conn.PingContext(ctx)
	if p_err != nil {
		log.Fatal("can not ping")
	}
	ctx, _ = context.WithTimeout(context.Background(), 20*time.Second)
	_, e_err := conn.ExecContext(ctx, "FLUSH TABLES;")
	if e_err != nil {
		log.Fatal("can not flush tables")
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
		log.Fatal("can not get master position")
	}
	for allrows.Next() {
		var v_bin_name string
		var v_bin_pos  string
		var v_str_1 string
		var v_str_2 string
		var v_str_3 string
		err := allrows.Scan(&v_bin_name, &v_bin_pos,&v_str_1,&v_str_2,&v_str_3)
		if err != nil {
			log.Fatal(err)
		}
		ret_val.Name=v_bin_name
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
	Status         bool
	cnxId          int
	Position       InfoMysqlPosition
}
type StatSqlSession struct {
	Cnt      int
	FileName string
	FilePos  int
}

func LockTableStartConsistenRead(infoconn chan InfoSqlSession, myId int, conn *sql.Conn, jobstart chan bool , jobsync chan int ) {
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
	// -------------------------------------------------------------------------------
	// we signal we are ready
	jobsync <- 1
	log.Printf("thread %d , we are ready\n", myId)
	// -------------------------------------------------------------------------------
	// we wait for signal
	<-jobstart
	// -------------------------------------------------------------------------------
	log.Printf("start stransaction for %d", myId)
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	_, s_err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT;")
	if s_err != nil {
		log.Fatalf("can not start transaction with consistent read\n%s\n", s_err.Error())
	}
	allrows, q_err := conn.QueryContext(ctx, "show master status;")
	if q_err != nil {
		log.Fatal("can not get master status ")
	}
	jobsync <- 2
	// -------------------------------------------------------------------------------
	for allrows.Next() {
		var v_bin_name string
		var v_bin_pos  string
		var v_str_1 string
		var v_str_2 string
		var v_str_3 string
		err := allrows.Scan(&v_bin_name, &v_bin_pos,&v_str_1,&v_str_2,&v_str_3)
		if err != nil {
			log.Fatal(err)
		}
		ret_val.Position.Name=v_bin_name
		ret_val.Position.Pos, _ = strconv.Atoi(v_bin_pos)
	}
	log.Printf("done start transaction for %d we are at %s@%d ", myId, ret_val.Position.Name, ret_val.Position.Pos)
	ret_val.Status = true
	infoconn <- ret_val
}

// ------------------------------------------------------------------------------------------
func GetaSynchronizedConnections(DbHost string, DbPort int, DbUsername string, DbUserPassword string, TargetCount int,ConDatabase string) ([]sql.Conn, StatSqlSession, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", DbUsername, DbUserPassword, DbHost, DbPort,ConDatabase))
	if err != nil {
		log.Fatal(err)
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
			log.Fatal(err)
		}
		db_conns[i] = first_conn
	}
	// --------------------------------------------------------------------------
	globallockchan := make(chan bool)
	globalposchan := make(chan InfoMysqlPosition )
	resultreadchan := make(chan InfoSqlSession, TargetCount*3-1)
	startreadchan := make(chan bool , TargetCount*3-1)
	syncreadchan := make(chan int , TargetCount*3-1)
	// -------------------------------------------
	for i := 1; i < TargetCount*3; i++ {
		go LockTableStartConsistenRead(resultreadchan, i, db_conns[i], startreadchan , syncreadchan )
	}
	// --------------------
	// we wait for TargetCount*3-1  feedback - ready to start a transaction
	for i := 1; i < TargetCount*3; i++ {
		<- syncreadchan
	}
	log.Print("everyone is ready")
	// --------------------
	// we start the read lock
	go LockTableWaitRelease(globallockchan, db_conns[0],globalposchan)
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
		<- syncreadchan
	}
	// --------------------
	// we release the global lock
	globallockchan <- true
	log.Print("ok as for release the global lock")
	// --------------------------------------------------------------------------
	var stats_ses []StatSqlSession
	db_sessions_filepos := make([]InfoSqlSession, TargetCount*3-1)
	<-globallockchan
	masterpos := <- globalposchan
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
	j := 0
	for foundRefPos == -1 && j < len(stats_ses) {
		if stats_ses[j].Cnt >= TargetCount {
			foundRefPos = j
		}
		j++
	}
	// --------------------
	log.Printf("we choose session with pos %s@%d", stats_ses[foundRefPos].FileName, stats_ses[foundRefPos].FilePos)
	log.Printf("master position was %s@%d", masterpos.Name, masterpos.Pos)
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
	return ret_dbconns, stats_ses[foundRefPos], nil
}

// ------------------------------------------------------------------------------------------
type columnInfo struct {
	colName     string
	colType     string
	isNullable  bool
	mustBeQuote bool
	haveFract   bool
	isKindChar  bool
}

type indexInfo struct {
	idxName     string
	cardinality int64
	columns     []columnInfo
}

type aTable struct {
	dbName         string
	tbName         string
}

type MetadataTable struct {
	dbName         string
	tbName         string
	cntRows        int64
	storageEng     string
	columnInfos    []columnInfo
	primaryKey     []string
	Indexes        []indexInfo
	fakePrimaryKey bool
	onError        int
}

// ------------------------------------------------------------------------------------------
func GetTableMetadataInfo(adbConn sql.Conn, dbName string, tableName string, guessPk bool) (MetadataTable, bool) {
	var result MetadataTable
	result.dbName = dbName
	result.tbName = tableName
	result.fakePrimaryKey = false
	result.onError = 0

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := adbConn.PingContext(ctx)
	if p_err != nil {
		log.Fatal("can not ping")
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err := adbConn.QueryContext(ctx, "select coalesce(TABLE_ROWS,-1),coalesce(ENGINE,'UNKNOW'),TABLE_TYPE from information_schema.tables WHERE table_schema = ? AND table_name = ?     ", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query information_schema.tables for %s.%s\n%s", dbName, tableName, q_err)
	}
	for q_rows.Next() {
		var typeTable string
		err := q_rows.Scan(&result.cntRows,&result.storageEng,&typeTable)
		if err != nil {
			log.Printf("can not query information_schema.tables for %s.%s", dbName, tableName)
			log.Fatal(err)
		}
		if result.storageEng != "InnoDB" {
			result.onError = result.onError | 4
		}
		if typeTable != "BASE TABLE" {
			result.onError = result.onError | 8
		}
	}

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err = adbConn.QueryContext(ctx, "select COLUMN_NAME , DATA_TYPE,IS_NULLABLE,IFNULL(DATETIME_PRECISION,-9999) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ?     ", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query information_schema.columns for %s.%s\n%s", dbName, tableName, q_err)
	}
	for q_rows.Next() {
		var a_col columnInfo
		var a_str string
		var a_int int
		err := q_rows.Scan(&a_col.colName, &a_col.colType, &a_str, &a_int)
		if err != nil {
			log.Fatal(err)
		}
		a_col.isNullable = (a_str == "YES")
		a_col.isKindChar = (a_col.colType == "char" || a_col.colType == "longtext" || a_col.colType == "mediumtext" || a_col.colType == "text" || a_col.colType == "tinytext" || a_col.colType == "varchar")
		a_col.mustBeQuote = a_col.isKindChar || (a_col.colType == "date" || a_col.colType == "datetime" || a_col.colType == "time" || a_col.colType == "timestamp")
		a_col.haveFract = (a_col.colType == "datetime" || a_col.colType == "timestamp" || a_col.colType == "time") && (a_int > 0)
		result.columnInfos = append(result.columnInfos, a_col)
	}

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err = adbConn.QueryContext(ctx, "select COLUMN_NAME  from INFORMATION_SCHEMA.STATISTICS WHERE table_schema = ? AND table_name = ?   and INDEX_NAME = 'PRIMARY' order by SEQ_IN_INDEX    ", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query INFORMATION_SCHEMA.STATISTICS  to get primary key info for %s.%s\n%s", dbName, tableName, q_err)
	}
	for q_rows.Next() {
		var a_str string
		err := q_rows.Scan(&a_str)
		if err != nil {
			log.Fatal(err)
		}
		result.primaryKey = append(result.primaryKey, a_str)
	}

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err = adbConn.QueryContext(ctx, "select COLUMN_NAME,CARDINALITY,INDEX_NAME  from INFORMATION_SCHEMA.STATISTICS WHERE  table_schema = ? and table_name = ? and INDEX_NAME != 'PRIMARY' order by INDEX_NAME,SEQ_IN_INDEX", dbName, tableName)
	if q_err != nil {
		log.Fatalf("can not query INFORMATION_SCHEMA.STATISTICS  to get index infos for %s.%s\n%s", dbName, tableName, q_err)
	}
	var idx_cur indexInfo
	for q_rows.Next() {
		var a_cardina int64
		var a_col_name string
		var a_idx_name string

		err := q_rows.Scan(&a_col_name, &a_cardina, &a_idx_name)
		if err != nil {
			log.Fatal(err)
		}
		if len(idx_cur.idxName) != 0 && idx_cur.idxName != a_idx_name {
			result.Indexes = append(result.Indexes, idx_cur)
			idx_cur.columns = nil
		}
		idx_cur.idxName = a_idx_name
		for _, v := range result.columnInfos {
			if v.colName == a_col_name {
				idx_cur.columns = append(idx_cur.columns, v)
			}
		}
		idx_cur.cardinality = a_cardina
	}
	if len(idx_cur.idxName) != 0 {
		result.Indexes = append(result.Indexes, idx_cur)
	}

	if len(result.primaryKey) == 0 {
		if !guessPk {
			result.onError=result.onError | 1
		} else {
			log.Printf("table %s.%s has no primary key\n", dbName, tableName)
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
				result.onError=result.onError | 2
			} else {
				// ---------------------------------------------------------
				result.fakePrimaryKey = true
				for _, colinfo := range result.Indexes[ix_pos].columns {
					result.primaryKey = append(result.primaryKey, colinfo.colName)
				}
				// ---------------------------------------------------------
			}
		}
	}

	return result, true
}

// ------------------------------------------------------------------------------------------
func GetListTables(adbConn sql.Conn, dbNames []string) []aTable {
	var result []aTable
	for _, v := range dbNames {
		result=append(result,GetListTablesBySchema(adbConn,v)[:]...)
	}
	return result
}
// ------------------------------------------------------------------------------------------
func GetListTablesBySchema(adbConn sql.Conn, dbName string) []aTable {
	var result []aTable

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := adbConn.PingContext(ctx)
	if p_err != nil {
		log.Fatal("can not ping")
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows, q_err := adbConn.QueryContext(ctx, "select TABLE_name from information_schema.tables WHERE table_schema = ? and TABLE_TYPE='BASE TABLE' order by table_name ", dbName)
	if q_err != nil {
		log.Fatalf("can not list tables from  information_schema.tables for %s\n%s", dbName, q_err)
	}
	for q_rows.Next() {
		var a_str string
		err := q_rows.Scan(&a_str)
		if err != nil {
			log.Fatal(err)
		}
		result = append(result, aTable{dbName:dbName,tbName:a_str})
	}
	return result
}

// ------------------------------------------------------------------------------------------
func GetMetadataInfo4Tables(adbConn sql.Conn, tableNames []aTable, guessPk bool) ([]MetadataTable, bool) {
	j := 0
	var result []MetadataTable
	for j < len(tableNames) {
		info, _ := GetTableMetadataInfo(adbConn, tableNames[j].dbName, tableNames[j].tbName, guessPk)
		result = append(result, info)
		j++
	}
	log.Printf("-------------------")
	cnt := 0
	for _ , v := range result {
		if v.onError & 1 == 1 {
			log.Printf("table %s.%s has no primary key\n", v.dbName, v.tbName)
			log.Print("you may want to use -guessprimarykey\n")
			cnt++
		}
		if v.onError & 2 == 2 {
			log.Printf("table %s.%s has no alternative indexes to use as a primary key\n", v.dbName, v.tbName)
			cnt++
		}
		if v.onError & 4 == 4 {
			log.Printf("table %s.%s is not a innodb table\n", v.dbName, v.tbName)
			cnt++
		}
		if v.onError & 8 == 8 {
			log.Printf("table %s.%s is not a regular table\n", v.dbName, v.tbName)
			cnt++
		}
	}
	if ( cnt > 0 ) {
		log.Fatalf("to many ERRORS")
	}
	return result, true
}

// ------------------------------------------------------------------------------------------
type tablechunk struct {
	table_id  int
	chunk_id  int64
	is_done   bool
	begin_val []string
	end_val   []string
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
			j := 0
			for j < ncolpkey {
				if j < i {
					sql_pred = sql_pred + fmt.Sprintf(" and ( `%s` = ? ) ", pkeyCols[j])
					sql_vals_indices = append(sql_vals_indices, j)
				}
				j++
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
func tableChunkBrowser(adbConn sql.Conn, tableInfos []MetadataTable, chunk2read chan tablechunk, sizeofchunk int64, readers_cnt int) {
	log.Printf("tableChunkBrowser  start\n")

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := adbConn.PingContext(ctx)
	if p_err != nil {
		log.Fatal("can not ping")
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	j := 0
	for j < len(tableInfos) {
		sql_lst_pk_cols := fmt.Sprintf("`%s`", tableInfos[j].primaryKey[0])
		c := 1
		for c < len(tableInfos[j].primaryKey) {
			sql_lst_pk_cols = sql_lst_pk_cols + "," + fmt.Sprintf("`%s`", tableInfos[j].primaryKey[c])
			c++
		}
		sql_full_tab_name := fmt.Sprintf("`%s`.`%s`", tableInfos[j].dbName, tableInfos[j].tbName)
		the_query := fmt.Sprintf("select %s from %s order by %s limit 1 ", sql_lst_pk_cols, sql_full_tab_name, sql_lst_pk_cols)
		log.Printf("table %s size pk %d query :  %s \n", sql_full_tab_name, c, the_query)
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
				log.Fatal(err)
			}
			row_cnt++
		}
		if row_cnt == 0 {
			log.Printf("table %s is empty \n", sql_full_tab_name)
			j++
			continue
		}
		start_pk_row := make([]string, c)
		for n, value := range a_sql_row {
			start_pk_row[n] = value.String
		}
		log.Printf("table %s first pk ( %s ) : %s\n", sql_full_tab_name, sql_lst_pk_cols, start_pk_row)
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
		var sql_vals_pk []any
		var end_pk_row []string
		for {
			if tableInfos[j].fakePrimaryKey {
				the_finish_query = fmt.Sprintf("select %s,@cnt as _cnt_pkey from ( select %s,@cnt:=@cnt+1 from %s , ( select @cnt := 0 ) c where %s order by %s limit %d ) e order by %s limit 1 ",
					sql_lst_pk_cols, sql_lst_pk_cols, sql_full_tab_name, sql_cond_pk, sql_lst_pk_cols, sizeofchunk, sql_order_pk_cols)
			} else {
				the_finish_query = fmt.Sprintf("select * from ( select %s from %s where %s order by %s limit %d ) e order by %s limit 1 ",
					sql_lst_pk_cols, sql_full_tab_name, sql_cond_pk, sql_lst_pk_cols, sizeofchunk, sql_order_pk_cols)
			}
			sql_vals_pk = generateValuesForPredicat(sql_val_indices_pk, start_pk_row)
			log.Printf("table %s query :  %s \n with %d params\n", sql_full_tab_name, the_finish_query, len(sql_vals_pk))
			ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
			q_rows, q_err = adbConn.QueryContext(ctx, the_finish_query, sql_vals_pk...)
			if q_err != nil {
				log.Fatalf("can not query table %s to get next pk aka ( %s )\n%s", sql_full_tab_name, sql_lst_pk_cols, q_err.Error())
			}
			for q_rows.Next() {
				err := q_rows.Scan(ptrs_nextpk...)
				if err != nil {
					log.Fatal(err)
				}
			}
			end_pk_row = make([]string, c)
			for n, value := range a_sql_row {
				end_pk_row[n] = value.String
			}
			if !tableInfos[j].fakePrimaryKey || !reflect.DeepEqual(start_pk_row, end_pk_row) || pk_cnt < sizeofchunk {
				break
			} else {
				sizeofchunk = sizeofchunk * 2
			}
		}
		log.Printf("table %s end pk ( %s ) scan pk %d : %s\n", sql_full_tab_name, sql_lst_pk_cols, pk_cnt, end_pk_row)
		// --------------------------------------------------------------------------
		var chunk_id int64 = 0
		if mode_debug {
			log.Printf("%s\n", reflect.DeepEqual(start_pk_row, end_pk_row))
		}
		for !reflect.DeepEqual(start_pk_row, end_pk_row) {
			chunk_id++
			var a_chunk tablechunk
			a_chunk.table_id = j
			a_chunk.chunk_id = chunk_id
			a_chunk.begin_val = start_pk_row
			a_chunk.end_val = end_pk_row
			a_chunk.is_done = false
			chunk2read <- a_chunk
			// ------------------------------------------------------------------
			for {
				ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
				start_pk_row = end_pk_row
				sql_vals_pk := generateValuesForPredicat(sql_val_indices_pk, start_pk_row)
				q_rows, q_err = adbConn.QueryContext(ctx, the_finish_query, sql_vals_pk...)
				if q_err != nil {
					log.Fatalf("can not query table %s to get next pk aka ( %s )\n%s", sql_full_tab_name, sql_lst_pk_cols, q_err.Error())
				}
				for q_rows.Next() {
					err := q_rows.Scan(ptrs_nextpk...)
					if err != nil {
						log.Fatal(err)
					}
				}
				end_pk_row = make([]string, c)
				for n, value := range a_sql_row {
					end_pk_row[n] = value.String
				}
				if !tableInfos[j].fakePrimaryKey || !reflect.DeepEqual(start_pk_row, end_pk_row) || pk_cnt < sizeofchunk {
					break
				} else {
					sizeofchunk = sizeofchunk * 2
					the_finish_query = fmt.Sprintf("select %s,@cnt as _cnt_pkey from ( select %s,@cnt:=@cnt+1 from %s , ( select @cnt := 0 ) c where %s order by %s limit %d ) e order by %s limit 1 ",
						sql_lst_pk_cols, sql_lst_pk_cols, sql_full_tab_name, sql_cond_pk, sql_lst_pk_cols, sizeofchunk, sql_order_pk_cols)
				}
			}
			if mode_debug {
				log.Printf("table %s query :  %s \n with %d params\nval for query: %s\nresult: %s\n", sql_full_tab_name, the_finish_query, len(sql_vals_pk), sql_vals_pk, end_pk_row)
				log.Printf("table %s pk interval : %s -> %s\n", sql_full_tab_name, start_pk_row, end_pk_row)
				log.Printf("%s\n", reflect.DeepEqual(start_pk_row, end_pk_row))
			}
			// ------------------------------------------------------------------
		}
		log.Printf("table scan is done for %s , pk col ( %s ) scan size pk %d last pk %s\n", sql_full_tab_name, sql_lst_pk_cols, pk_cnt, end_pk_row)
		// --------------------------------------------------------------------------
		chunk_id++
		var a_chunk tablechunk
		a_chunk.table_id = j
		a_chunk.chunk_id = chunk_id
		a_chunk.begin_val = start_pk_row
		a_chunk.end_val = end_pk_row
		a_chunk.is_done = false
		chunk2read <- a_chunk
		// --------------------------------------------------------------------------
		j++
	}
	log.Printf("tableChunkBrowser finish\n")
	// ----------------------------------------------------------------------------------
	var a_chunk tablechunk
	a_chunk.table_id = -1
	a_chunk.chunk_id = -1
	a_chunk.begin_val = make([]string, 0)
	a_chunk.end_val = make([]string, 0)
	a_chunk.is_done = true
	j = 0
	for j < readers_cnt {
		chunk2read <- a_chunk
		j++
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
func ChunkReaderDumpProcess(dumpmode string, q_rows *sql.Rows, row_cnt *int, query_row_count *int, a_quote_info []bool, a_fract_info []bool, a_char_info []bool, a_row []any, a_sql_row []*sql.NullString, a_table_info MetadataTable, sql_tab_cols string, sql_val_cols string, ptrs []any, insert_size int, cur_iow io.Writer) {
	// --------------------------------------------------------------------------
	if dumpmode == "sql" {
		for q_rows.Next() {
			err := q_rows.Scan(ptrs...)
			if err != nil {
				log.Fatal(err)
			}
			*row_cnt++
			*query_row_count++
			// ----------------------------------------------------------
			for n, value := range a_sql_row {
				if value == nil {
					a_row[n] = "null"
				} else {
					if a_quote_info[n] {
						a_row[n] = fmt.Sprintf("\"%s\"", value.String)
					} else {
						a_row[n] = value.String
					}
				}
			}
			// ----------------------------------------------------------
			if *row_cnt == 1 {
				_, err := io.WriteString(cur_iow, fmt.Sprintf("insert into `%s`(%s) values ", a_table_info.tbName, sql_tab_cols))
				if err != nil {
					log.Fatal(err)
				}
				io.WriteString(cur_iow, fmt.Sprintf("("+sql_val_cols+")", a_row...))
			} else {
				io.WriteString(cur_iow, fmt.Sprintf(",("+sql_val_cols+")", a_row...))
			}
			// ----------------------------------------------------------
			if *row_cnt > insert_size {
				io.WriteString(cur_iow, ";\n")
				*row_cnt = 0
			}
		}
		if *row_cnt > 0 {
			io.WriteString(cur_iow, ";\n")
		}
	}
	// --------------------------------------------------------------------------
	if dumpmode == "csv" {
		for q_rows.Next() {
			err := q_rows.Scan(ptrs...)
			if err != nil {
				log.Fatal(err)
			}
			*row_cnt++
			*query_row_count++
			// ----------------------------------------------------------
			for n, value := range a_sql_row {
				if value == nil {
					if a_char_info[n] {
						a_row[n] = "\\N"
					} else {
						a_row[n] = ""
					}
				} else {
					if a_quote_info[n] && (strings.IndexAny(value.String, "\n,\"") != -1) {
						a_row[n] = fmt.Sprintf("\"%s\"", strings.ReplaceAll(value.String, "\"", "\"\""))
					} else if a_fract_info[n] {
						timeSec, timeFract, dotFound := strings.Cut(value.String, ".")
						if dotFound {
							timeFract = strings.TrimRight(timeFract, "0")
							if len(timeFract) == 1 {
								timeFract = timeFract + "0"
							}
							a_row[n] = timeSec + "." + timeFract
						} else {
							a_row[n] = value.String
						}
					} else {
						a_row[n] = value.String
					}
				}
			}
			// ----------------------------------------------------------
			io.WriteString(cur_iow, fmt.Sprintf(sql_val_cols+"\n", a_row...))
			// ----------------------------------------------------------
		}
	}
}

// ------------------------------------------------------------------------------------------
func tableChunkReader(chunk2read chan tablechunk, adbConn sql.Conn, tableInfos []MetadataTable, id int, dumpfiletemplate string, dumpmode string, dumpcompress string, insert_size int) {
	log.Printf("tableChunkReader[%d] start\n", id)

	var last_tableid int = -1
	var sql_cond_lower_pk string
	var sql_cond_upper_pk string
	var sql_cond_equal_pk string
	var sql_val_indices_pk []int
	var sql_val_indices_equal_pk []int
	var sql_full_tab_name string
	var sql_pk_cols string
	var sql_tab_cols string
	var sql_val_cols string
	var the_reader_interval_query string
	var the_reader_equality_query string
	var zst_enc *zstd.Encoder
	var und_fh *os.File
	var err error
	a_row := make([]any, 0)
	a_quote_info := make([]bool, 0)
	a_char_info := make([]bool, 0)
	a_fract_info := make([]bool, 0)
	a_sql_row := make([]*sql.NullString, 1)
	ptrs := make([]any, 1)
	for i := range a_sql_row {
		ptrs[i] = &a_sql_row[i]
	}
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
			log.Fatal(err)
		} else {
			fh.Close()
		}
		file_is_empty = append(file_is_empty, true)
		file_name = append(file_name, fname)
	}
	a_chunk := <-chunk2read
	for true {
		if a_chunk.is_done {
			if und_fh != nil {
				if zst_enc != nil {
					zst_enc.Close()
				}
				und_fh.Close()
			}
			break
		}
		if last_tableid != a_chunk.table_id {
			// ------------------------------------------------------------------
			last_tableid = a_chunk.table_id
			sql_cond_lower_pk, sql_val_indices_pk = generatePredicat(tableInfos[last_tableid].primaryKey, true)
			sql_cond_upper_pk, sql_val_indices_pk = generatePredicat(tableInfos[last_tableid].primaryKey, false)
			sql_cond_equal_pk, sql_val_indices_equal_pk = generateEqualityPredicat(tableInfos[last_tableid].primaryKey)
			sql_full_tab_name = fmt.Sprintf("`%s`.`%s`", tableInfos[last_tableid].dbName, tableInfos[last_tableid].tbName)
			sql_pk_cols = fmt.Sprintf("`%s`", tableInfos[last_tableid].primaryKey[0])
			cpk := 1
			for cpk < len(tableInfos[last_tableid].primaryKey) {
				sql_pk_cols = sql_pk_cols + "," + fmt.Sprintf("`%s`", tableInfos[last_tableid].primaryKey[cpk])
				cpk++
			}
			sql_tab_cols = fmt.Sprintf("`%s`", tableInfos[last_tableid].columnInfos[0].colName)
			sql_val_cols = "%s"
			ctab := 1
			for ctab < len(tableInfos[last_tableid].columnInfos) {
				sql_tab_cols = sql_tab_cols + "," + fmt.Sprintf("`%s`", tableInfos[last_tableid].columnInfos[ctab].colName)
				sql_val_cols = sql_val_cols + "," + "%s"
				ctab++
			}
			the_reader_interval_query = fmt.Sprintf("select %s from %s where ( %s ) and ( %s) order by %s ", sql_tab_cols, sql_full_tab_name, sql_cond_lower_pk, sql_cond_upper_pk, sql_pk_cols)
			the_reader_equality_query = fmt.Sprintf("select %s from %s where ( %s ) order by %s ", sql_tab_cols, sql_full_tab_name, sql_cond_equal_pk, sql_pk_cols)
			a_sql_row = make([]*sql.NullString, ctab)
			ptrs = make([]any, ctab)
			a_quote_info = make([]bool, ctab)
			a_char_info = make([]bool, ctab)
			a_fract_info = make([]bool, ctab)
			for i := range a_sql_row {
				ptrs[i] = &a_sql_row[i]
				a_quote_info[i] = tableInfos[last_tableid].columnInfos[i].mustBeQuote
				a_char_info[i] = tableInfos[last_tableid].columnInfos[i].isKindChar
				a_fract_info[i] = tableInfos[last_tableid].columnInfos[i].haveFract
			}
			a_row = make([]any, ctab)
			// ------------------------------------------------------------------
			if und_fh != nil {
				if zst_enc != nil {
					zst_enc.Close()
				}
				und_fh.Close()
			}
			fname := file_name[last_tableid]
			und_fh, err = os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				log.Fatal(err)
			}
			if dumpcompress == "zstd" {
				zst_enc, err = zstd.NewWriter(und_fh, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
				if err != nil {
					log.Fatal(err)
				}
			}
			if file_is_empty[last_tableid] {
				if dumpcompress == "zstd" {
					ChunkReaderDumpHeader(dumpmode, zst_enc, sql_tab_cols)
				} else {
					ChunkReaderDumpHeader(dumpmode, und_fh, sql_tab_cols)
				}
				file_is_empty[last_tableid] = false
			}
			// ------------------------------------------------------------------
		}
		// --------------------------------------------------------------------------
		// log.Printf("[%02d] table %03d chunk %06d %s -> %s\n",id,a_chunk.table_id,a_chunk.chunk_id,a_chunk.begin_val,a_chunk.end_val)
		// --------------------------------------------------------------------------
		var sql_vals_pk []any
		var the_query *string
		if reflect.DeepEqual(a_chunk.begin_val, a_chunk.end_val) {
			sql_vals_pk = generateValuesForPredicat(sql_val_indices_equal_pk, a_chunk.begin_val)
			the_query = &the_reader_equality_query
		} else {
			sql_vals_pk = generateValuesForPredicat(sql_val_indices_pk, a_chunk.begin_val)
			sql_vals_pk = append(sql_vals_pk, generateValuesForPredicat(sql_val_indices_pk, a_chunk.end_val)...)
			the_query = &the_reader_interval_query
		}
		ctx, _ := context.WithTimeout(context.Background(), 16*time.Second)
		q_rows, q_err := adbConn.QueryContext(ctx, *the_query, sql_vals_pk...)
		if q_err != nil {
			log.Printf("table %s chunk id: %d chunk query :  %s \n with %d params\nval for query: %s\n", sql_full_tab_name, a_chunk.chunk_id, *the_query, len(sql_vals_pk), sql_vals_pk)
			log.Fatalf("can not query table %s to read the chunks\n%s", sql_full_tab_name, sql_vals_pk, q_err.Error())
		}
		if mode_debug {
			log.Printf("table %s chunk id: %d chunk query :  %s \n with %d params\nval for query: %s\nquote info: %s\n", sql_full_tab_name, a_chunk.chunk_id, *the_query, len(sql_vals_pk), sql_vals_pk, a_quote_info)
		}
		row_cnt := 0
		query_row_count := 0
		// --------------------------------------------------------------------------
		if dumpcompress == "zstd" {
			ChunkReaderDumpProcess(dumpmode, q_rows, &row_cnt, &query_row_count, a_quote_info, a_fract_info, a_char_info, a_row, a_sql_row, tableInfos[last_tableid], sql_tab_cols, sql_val_cols, ptrs, insert_size, zst_enc)
		} else {
			ChunkReaderDumpProcess(dumpmode, q_rows, &row_cnt, &query_row_count, a_quote_info, a_fract_info, a_char_info, a_row, a_sql_row, tableInfos[last_tableid], sql_tab_cols, sql_val_cols, ptrs, insert_size, und_fh)
		}
		// --------------------------------------------------------------------------
		if mode_debug {
			log.Printf("table %s chunk query :  %s \n with %d params\nval for query: %s\nrows cnt: %d\n", sql_full_tab_name, *the_query, len(sql_vals_pk), sql_vals_pk, row_cnt)
		}
		// --------------------------------------------------------------------------
		a_chunk = <-chunk2read
	}
	log.Printf("tableChunkReader[%d] finish\n", id)
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
	arg_db_port := flag.Int("port", 3306, "the database port")
	arg_db_host := flag.String("host", "127.0.0.1", "the database host")
	arg_db_user := flag.String("user", "mysql", "the database connection user")
	arg_db_pasw := flag.String("pwd", "", "the database connection password")
	arg_db_parr := flag.Int("parallel", 10, "number of workers")
	arg_chunk_size := flag.Int("chunksize", 10000, "rows count when reading")
	arg_insert_size := flag.Int("insertsize", 100, "rows count for each insert")
	arg_dumpfile := flag.String("dumpfile", "dump_%d_%t_%p.%m", "sql dump of tables")
	arg_dumpmode := flag.String("dumpmode", "sql", "format of the dump , csv / sql ")
	arg_dumpcompress := flag.String("dumpcompress", "", "which compression format to use , zstd")
	// ------------
	var arg_dbs arrayFlags
	flag.Var(&arg_dbs,"db","database(s) of tables to dump")
	// ------------
	var arg_tables2dump arrayFlags
	flag.Var(&arg_tables2dump, "table", "table to dump")
	// ------------
	arg_guess_pk := flag.Bool("guessprimarykey", false, "guess a primary key in case table does not have one")
	arg_all_tables := flag.Bool("alltables", false, "all tables of the specified database")
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
	if arg_dbs != nil && len(arg_dbs) > 1 && arg_tables2dump != nil {
		log.Printf("can not specify multiple databases with a list of tables")
		flag.Usage()
		os.Exit(5)
	}
	if len(*arg_dumpfile) == 0 {
		flag.Usage()
		os.Exit(6)
	}
	if len(*arg_dumpmode) != 0 && (*arg_dumpmode != "sql" && *arg_dumpmode != "csv") {
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
	mode_debug = *arg_debug
	// ----------------------------------------------------------------------------------
	conDb, _ , _ := GetaSynchronizedConnections(*arg_db_host, *arg_db_port, *arg_db_user, *arg_db_pasw, *arg_db_parr,arg_dbs[0])
	// ----------------------------------------------------------------------------------
	var tables2dump []aTable
	if arg_tables2dump == nil {
		tables2dump = GetListTables(conDb[0], arg_dbs)
	} else {
		for _ , t := range arg_tables2dump {
			tables2dump = append(tables2dump,aTable{dbName:arg_dbs[0],tbName:t})
		}
	}
	log.Print(tables2dump)
	r, _ := GetMetadataInfo4Tables(conDb[0], tables2dump, *arg_guess_pk)
	log.Printf("tables infos  => %s", r)
	// ----------------------------------------------------------------------------------
	var wg sync.WaitGroup
	pk_chunks_to_read := make(chan tablechunk, 1000)
	wg.Add(1)
	go func() {
		defer wg.Done()
		tableChunkBrowser(conDb[0], r, pk_chunks_to_read, int64(*arg_chunk_size), len(conDb)-1)
	}()
	j := 1
	for j < len(conDb) {
		wg.Add(1)
		go func(adbConn sql.Conn, id int) {
			defer wg.Done()
			tableChunkReader(pk_chunks_to_read, adbConn, r, id, *arg_dumpfile, *arg_dumpmode, *arg_dumpcompress, *arg_insert_size)
		}(conDb[j], j)
		j++
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
	// ----------------------------------------------------------------------------------
}

// ------------------------------------------------------------------------------------------
