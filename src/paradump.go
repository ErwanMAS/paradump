// ------------------------------------------------------------------------------------------
package main

import (
	"time"
	"sync"
	"reflect"
//	"io/ioutil"
	"strconv"
	"context"
	"fmt"
	"os"
	"log"
	"flag"
	"strings"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)
/* ------------------------------------------------------------------------------------------
   ParaDump is a tool that will create a dump of mysql in table , by using multiple threads
   to read tables .

   ------------------------------------------------------------------------------------------

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
func LockTableWaitRelease( jobsync chan bool , conn *sql.Conn , LockTable string  , LockDatabase string ) {
	// --------------------
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := conn.PingContext(ctx)
	if p_err != nil {
		log.Fatal("can not ping")
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	_ , e_err := conn.ExecContext(ctx,"START TRANSACTION;")
	if e_err != nil {
		log.Fatal("can not start transaction")
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	_ , l_err := conn.ExecContext(ctx,fmt.Sprintf("LOCK TABLES %s.%s WRITE;",LockDatabase,LockTable))
	if l_err != nil {
		log.Fatalf("can not lock able in write \n%s\n",l_err.Error())
	}
	jobsync <-true
	<- jobsync
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	_ , r_err := conn.ExecContext(ctx,"UNLOCK TABLES;")
	if r_err != nil {
		log.Fatalf("can not unlock tabless\n%s\n",r_err.Error())
	}
	jobsync <-true
}
// ------------------------------------------------------------------------------------------
type InfoSqlSession struct{
	Status    bool
	cnxId     int
	FileName  string
	FilePos   int
}
type StatSqlSession struct{
	Cnt       int
	FileName  string
	FilePos   int
}

func LockTableStartConsistenRead( infoconn chan InfoSqlSession , myId int , conn *sql.Conn , LockTable string  , LockDatabase string ) {
	var ret_val InfoSqlSession
	ret_val.Status=false
	ret_val.cnxId=myId
	// --------------------
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := conn.PingContext(ctx)
	if p_err != nil {
		log.Fatal("can not ping")
	}
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
	_ , e_err := conn.ExecContext(ctx,"SET NAMES utf8mb4")
	if e_err != nil {
		log.Printf("thread %d , can not set NAMES for the session\n%s\n",myId,e_err.Error())
		infoconn <- ret_val
		return
	}
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
	_ , t_err := conn.ExecContext(ctx,"SET TIME_ZONE='+00:00' ")
	if t_err != nil {
		log.Printf("thread %d , can not set TIME_ZONE for the session\n%s\n",myId,t_err.Error())
		infoconn <- ret_val
		return
	}
	log.Printf("start lock read for %d",myId)
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	_ , l_err := conn.ExecContext(ctx,fmt.Sprintf("LOCK TABLES %s.%s READ;",LockDatabase,LockTable))
	if l_err != nil {
		log.Printf("thread %d , can not lock tables in read \n%s\n",myId,l_err.Error())
		infoconn <- ret_val
		return
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	_ , s_err := conn.ExecContext(ctx,"START TRANSACTION WITH CONSISTENT SNAPSHOT;")
	if s_err != nil {
		log.Fatalf("can not start transaction with consistent read\n%s\n",s_err.Error())
	}
	allrows , q_err := conn.QueryContext(ctx,"SHOW STATUS LIKE 'binlog_snapshot_%';")
	if q_err != nil {
		log.Fatal("can not get binlog_snapshot_XX values")
	}
	for allrows.Next() {
		var v_name string
		var v_value string
		err := allrows.Scan(&v_name, &v_value)
		if err != nil {
			log.Fatal(err)
		}
		if v_name == "Binlog_snapshot_file" {
			ret_val.FileName=v_value
		}
		if v_name == "Binlog_snapshot_position" {
			ret_val.FilePos,_=strconv.Atoi(v_value)
		}
	}
	log.Printf("done start transaction for %d we are at %s@%d",myId,ret_val.FileName,ret_val.FilePos)
	ret_val.Status=true
	infoconn <- ret_val
}
// ------------------------------------------------------------------------------------------
func GetaSynchronizedConnections( DbHost string , DbPort int , DbUsername string , DbUserPassword string , TargetCount int , LockTable string  , LockDatabase string ) ([]sql.Conn,StatSqlSession, error) {

	db , err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",DbUsername,DbUserPassword,DbHost,DbPort,LockDatabase))
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(TargetCount*3)
	db.SetMaxIdleConns(TargetCount*3)
	// --------------------
	var ctx context.Context
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	// --------------------
	db_conns := make([]*sql.Conn, TargetCount*3 )
	for i:=0; i<TargetCount*3; i++ {
		first_conn , err := db.Conn(ctx)
		if err != nil {
			log.Fatal(err)
		}
		db_conns[i] = first_conn
	}
	// --------------------
	tablelockchan := make(chan bool)
	startsesschan := make(chan InfoSqlSession, TargetCount*3-1)
	go LockTableWaitRelease(tablelockchan,db_conns[0],LockTable,LockDatabase)
	<- tablelockchan
	log.Print("ok for master lock")
	for i:=1; i<TargetCount*3; i++ {
		go LockTableStartConsistenRead(startsesschan,i,db_conns[i],LockTable,LockDatabase)
	}
	tablelockchan <- true
	var stats_ses [] StatSqlSession
	var db_sessions_filepos = make([]InfoSqlSession, TargetCount*3-1)
	<- tablelockchan
	for i:=1; i<TargetCount*3; i++ {
		db_sessions_filepos[i-1] = <- startsesschan
		foundidx := -1
		for j:=0; j<len(stats_ses); j++ {
			if foundidx == -1  && stats_ses[j].FileName == db_sessions_filepos[i-1].FileName && stats_ses[j].FilePos == db_sessions_filepos[i-1].FilePos {
				stats_ses[j].Cnt++
				foundidx=j
			}
		}
		if foundidx == -1 {
			stats_ses = append ( stats_ses , StatSqlSession{ Cnt : 1 , FileName: db_sessions_filepos[i-1].FileName , FilePos : db_sessions_filepos[i-1].FilePos } )
		}
	}
	// --------------------
	log.Printf("we collected infos about %d sessions differents postions count is %d",len(db_sessions_filepos),len(stats_ses))
	foundRefPos := -1
	j := 0
	for foundRefPos ==-1 && j <len(stats_ses)  {
		if stats_ses[j].Cnt >= TargetCount {
			foundRefPos = j
			log.Printf("we choose session with pos %s @ %d",stats_ses[foundRefPos].FileName,stats_ses[foundRefPos].FilePos)
		}
		j++
	}
	var ret_dbconns []sql.Conn
	if foundRefPos >= 0 {
		for i:=0; i<TargetCount*3-1; i++ {
			if db_sessions_filepos[i].FileName ==  stats_ses[foundRefPos].FileName && db_sessions_filepos[i].FilePos ==  stats_ses[foundRefPos].FilePos  && len(ret_dbconns) < TargetCount {
				ret_dbconns=append(ret_dbconns ,*db_conns[db_sessions_filepos[i].cnxId])
				db_conns[db_sessions_filepos[i].cnxId]=nil
			}
		}
	}
	for i:=0; i<TargetCount*3; i++ {
		if db_conns[i] != nil {
			db_conns[i].Close()
		}
	}
	// --------------------
	return ret_dbconns , stats_ses[foundRefPos], nil
}
// ------------------------------------------------------------------------------------------
func CheckSessions( dbConns []sql.Conn , infoPos StatSqlSession ) bool {
	j := 0
	bug_check := 0
	for j <len(dbConns)  {
		conn:=dbConns[j]
		// --------------------
		ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
		p_err := conn.PingContext(ctx)
		if p_err != nil {
			log.Fatal("can not ping")
		}
		ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
		allrows , q_err := conn.QueryContext(ctx,"SHOW STATUS LIKE 'binlog_snapshot_%';")
		if q_err != nil {
			log.Fatal("can not get binlog_snapshot_XX values")
		}
		for allrows.Next() {
			var v_name string
			var v_value string
			err := allrows.Scan(&v_name, &v_value)
			if err != nil {
				log.Fatal(err)
			}
			if v_name == "Binlog_snapshot_file" && infoPos.FileName != v_value {
				log.Printf(" Binlog_snapshot_file has moved")
				bug_check++
			}
			if v_name == "Binlog_snapshot_position" {
				a_int,_ :=strconv.Atoi(v_value)
				if ( infoPos.FilePos != a_int ) {
					log.Printf(" Binlog_snapshot_file has moved")
					bug_check++
				}
			}
		}
		j++
	}
	return bug_check == 0
}
// ------------------------------------------------------------------------------------------
type columnInfo struct {
	colName      string
	colType      string
	isNullable   bool
	mustBeQuote  bool
}

type indexInfo struct {
	idxName      string
	cardinality  int64
	columns      []string
}

type MetadataTable struct{
	dbName       string
	tbName       string
	cntRows      int64
	columnInfos  []columnInfo
	primaryKey   []string
	Indexes      []indexInfo
}
// ------------------------------------------------------------------------------------------
func GetTableMetadataInfo( adbConn  sql.Conn , dbName string , tableName string ) (MetadataTable,bool) {
	var result MetadataTable
	result.dbName=dbName ;
	result.tbName=tableName ;

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := adbConn.PingContext(ctx)
	if p_err != nil {
		log.Fatal("can not ping")
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows , q_err := adbConn.QueryContext(ctx,"select TABLE_ROWS  from information_schema.tables WHERE table_schema = ? AND table_name = ?     ",dbName,tableName)
	if q_err != nil {
		log.Fatal("can not query information_schema.tables for %s.%s\n%s",dbName,tableName,q_err)
	}
	for q_rows.Next() {
		err := q_rows.Scan(&result.cntRows)
		if err != nil {
			log.Fatal(err)
		}
	}

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows , q_err = adbConn.QueryContext(ctx,"select COLUMN_NAME , DATA_TYPE,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ?     ",dbName,tableName)
	if q_err != nil {
		log.Fatal("can not query information_schema.columns for %s.%s\n%s",dbName,tableName,q_err)
	}
	for q_rows.Next() {
		var a_col columnInfo
		var a_str string
		err := q_rows.Scan(&a_col.colName,&a_col.colType,&a_str)
		if err != nil {
			log.Fatal(err)
		}
		a_col.isNullable = (a_str == "YES" )
		a_col.mustBeQuote = ( a_col.colType == "char"||  a_col.colType == "longtext"||  a_col.colType == "mediumtext"||  a_col.colType == "text"||  a_col.colType == "tinytext"||  a_col.colType == "varchar"||  a_col.colType == "date"||  a_col.colType =="datetime"||  a_col.colType =="timestamp" )
		result.columnInfos = append ( result.columnInfos , a_col)
	}

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows , q_err = adbConn.QueryContext(ctx,"select COLUMN_NAME  from INFORMATION_SCHEMA.STATISTICS WHERE table_schema = ? AND table_name = ?   and INDEX_NAME = 'PRIMARY' order by SEQ_IN_INDEX    ",dbName,tableName)
	if q_err != nil {
		log.Fatal("can not query INFORMATION_SCHEMA.STATISTICS  to get primary key info for %s.%s\n%s",dbName,tableName,q_err)
	}
	for q_rows.Next() {
		var a_str string
		err := q_rows.Scan(&a_str)
		if err != nil {
			log.Fatal(err)
		}
		result.primaryKey = append ( result.primaryKey , a_str)
	}

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)

	q_rows , q_err = adbConn.QueryContext(ctx,"select COLUMN_NAME,CARDINALITY,INDEX_NAME  from INFORMATION_SCHEMA.STATISTICS WHERE  table_schema = ? and table_name = ? and INDEX_NAME != 'PRIMARY' order by INDEX_NAME,SEQ_IN_INDEX",dbName,tableName)
	if q_err != nil {
		log.Fatal("can not query INFORMATION_SCHEMA.STATISTICS  to get index infos for %s.%s\n%s",dbName,tableName,q_err)
	}
	var idx_cur indexInfo
	for q_rows.Next() {
		var a_cardina  int64
		var a_col_name string
		var a_idx_name string

		err := q_rows.Scan(&a_col_name , &a_cardina , &a_idx_name )
		if err != nil {
			log.Fatal(err)
		}
		if len(idx_cur.idxName) != 0 && idx_cur.idxName != a_idx_name {
			result.Indexes = append ( result.Indexes , idx_cur )
			idx_cur.columns= nil
		}
		idx_cur.idxName= a_idx_name
		idx_cur.columns= append ( idx_cur.columns , a_col_name )
		idx_cur.cardinality = a_cardina
	}
	if len(idx_cur.idxName) != 0 {
		result.Indexes = append ( result.Indexes , idx_cur )
	}
	return result , true
}
// ------------------------------------------------------------------------------------------
func GetMetadataInfo4Tables( adbConn sql.Conn , dbName string , tableNames []string ) ([]MetadataTable,bool) {
	j := 0
	var result []MetadataTable
	for j <len(tableNames)  {
		info , _ := GetTableMetadataInfo ( adbConn , dbName , tableNames[j] )
		result = append ( result , info )
		j++
	}
	return result , true
}
// ------------------------------------------------------------------------------------------
type tablechunk struct {
	table_id  int
	chunk_id  int64
	begin_val []string
	end_val   []string
}
// ------------------------------------------------------------------------------------------
func generateValuesForPredicat( indices []int , boundValues []string ) []any {
	sql_vals:=make([]any,0)
	for _ , v := range indices {
		sql_vals=append(sql_vals,boundValues[v])
	}
	return sql_vals
}
// ------------------------------------------------------------------------------------------
// lower bound is inclusive
// upper bound is exclusive
//
func generatePredicat( pkeyCols []string , lowerbound  bool)  ( string , []int ) {
        var sql_pred  string
        sql_vals_indices:=make([]int,0)
	var ncolpkey = len(pkeyCols) -1
	op_1 := " <  "
	op_o := " <  "
	if lowerbound {
		op_1 = " >=  "
		op_o = " >   "
	}
	sql_pred = " ( "
	for i, _ := range pkeyCols {
		if ncolpkey  == i {
			sql_pred = sql_pred + fmt.Sprintf(" ( `%s` %s ? ) " ,pkeyCols[i],op_1)
		} else {
			sql_pred = sql_pred + fmt.Sprintf(" ( `%s` %s ? ) " ,pkeyCols[i],op_o)
		}
		sql_vals_indices=append(sql_vals_indices,i)
		if ncolpkey > 0 {
			j:=0
			for j < ncolpkey {
				if j < i {
					sql_pred = sql_pred + fmt.Sprintf( " and ( `%s` = ? ) ",pkeyCols[j])
					sql_vals_indices=append(sql_vals_indices,j)
				}
				j++
			}
		}
		if i < ncolpkey {
			sql_pred = sql_pred  + " ) or ( "
		}
	}
	sql_pred = sql_pred  + " ) "
	return sql_pred,sql_vals_indices
}
// ------------------------------------------------------------------------------------------
func generateEqualityPredicat( pkeyCols []string )  ( string , []int ) {
        var sql_pred  string
        sql_vals_indices:=make([]int,0)
	sql_pred = " ( "
	for i, _ := range pkeyCols {
		if i != 0 {
			sql_pred = sql_pred + " and "
		}
		sql_pred = sql_pred + fmt.Sprintf(" ( `%s` = ? ) " ,pkeyCols[i])
		sql_vals_indices=append(sql_vals_indices,i)
	}
	sql_pred = sql_pred  + " ) "
	return sql_pred,sql_vals_indices
}
// ------------------------------------------------------------------------------------------
func tableChunkBrowser(adbConn  sql.Conn , tableInfos []MetadataTable , chunk2read chan tablechunk , sizeofchunk int ) {

	log.Printf("tableChunkBrowser  start\n")

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	p_err := adbConn.PingContext(ctx)
	if p_err != nil {
		log.Fatal("can not ping")
	}
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
        j:=0
	for j <len(tableInfos)  {
		sql_lst_pk_cols := fmt.Sprintf("`%s`",tableInfos[j].primaryKey[0])
		c:=1
		for c < len(tableInfos[j].primaryKey) {
			sql_lst_pk_cols = sql_lst_pk_cols + "," + fmt.Sprintf("`%s`",tableInfos[j].primaryKey[c])
			c++
		}
		sql_full_tab_name := fmt.Sprintf("`%s`.`%s`",tableInfos[j].dbName,tableInfos[j].tbName)
		the_query := fmt.Sprintf("select %s from %s order by %s limit 1 ",sql_lst_pk_cols,sql_full_tab_name,sql_lst_pk_cols)
		log.Printf("table %s query :  %s \n",sql_full_tab_name,the_query )
		q_rows , q_err := adbConn.QueryContext(ctx,the_query)
		if q_err != nil {
			log.Fatalf("can not query table %s to get first pk aka ( %s )\n%s",sql_full_tab_name,sql_lst_pk_cols,q_err.Error())
		}
		a_sql_row := make([]*sql.NullString,c)
		ptrs := make([]interface{},c)
		for i, _ := range a_sql_row {
			ptrs[i] = &a_sql_row[i]
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
			log.Printf("table %s is empty \n",sql_full_tab_name)
			j++
			continue
		}
		start_pk_row := make([]string, c )
		for n , value := range a_sql_row {
			start_pk_row[n] = value.String
		}
		log.Printf("table %s first pk ( %s ) : %s\n",sql_full_tab_name,sql_lst_pk_cols , start_pk_row)
		// --------------------------------------------------------------------------
		sql_cond_pk , sql_val_indices_pk :=  generatePredicat ( tableInfos[j].primaryKey , true )
		var sql_order_pk_cols string
		for i , v := range tableInfos[j].primaryKey {
			if i != 0 {
				sql_order_pk_cols = sql_order_pk_cols + ","
			}
			sql_order_pk_cols = sql_order_pk_cols + fmt.Sprintf("`%s` desc ",v)
		}
		the_finish_query := fmt.Sprintf("select * from ( select %s from %s where %s order by %s limit %d ) e order by %s limit 1 ",
			sql_lst_pk_cols,sql_full_tab_name,sql_cond_pk ,sql_lst_pk_cols , sizeofchunk , sql_order_pk_cols )
		sql_vals_pk := generateValuesForPredicat ( sql_val_indices_pk , start_pk_row )
		log.Printf("table %s query :  %s \n with %d params\n",sql_full_tab_name,the_finish_query,len(sql_vals_pk))
		q_rows , q_err = adbConn.QueryContext(ctx,the_finish_query,sql_vals_pk...)
		if q_err != nil {
			log.Fatalf("can not query table %s to get next pk aka ( %s )\n%s",sql_full_tab_name,sql_lst_pk_cols,q_err.Error())
		}
		for q_rows.Next() {
			err := q_rows.Scan(ptrs...)
			if err != nil {
				log.Fatal(err)
			}
		}
		end_pk_row := make([]string, c )
		for n , value := range a_sql_row {
			end_pk_row[n] = value.String
		}
		log.Printf("table %s end pk ( %s ) : %s\n",sql_full_tab_name,sql_lst_pk_cols , end_pk_row)
		// --------------------------------------------------------------------------
		var chunk_id int64 = 0
		log.Printf("%s\n",reflect.DeepEqual(start_pk_row,end_pk_row))
		for ! reflect.DeepEqual(start_pk_row,end_pk_row) {
			chunk_id++
			var a_chunk tablechunk
			a_chunk.table_id=j
			a_chunk.chunk_id=chunk_id
			a_chunk.begin_val=start_pk_row
			a_chunk.end_val=end_pk_row
			chunk2read <-  a_chunk
			// ------------------------------------------------------------------
			ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
			start_pk_row = end_pk_row
			sql_vals_pk := generateValuesForPredicat ( sql_val_indices_pk , start_pk_row )
			q_rows , q_err = adbConn.QueryContext(ctx,the_finish_query,sql_vals_pk...)
			if q_err != nil {
				log.Fatalf("can not query table %s to get next pk aka ( %s )\n%s",sql_full_tab_name,sql_lst_pk_cols,q_err.Error())
			}
			for q_rows.Next() {
				err := q_rows.Scan(ptrs...)
				if err != nil {
					log.Fatal(err)
				}
			}
			end_pk_row = make([]string, c )
			for n , value := range a_sql_row {
				end_pk_row[n] = value.String
			}
			if ( mode_debug ) {
				log.Printf("table %s query :  %s \n with %d params\nval for query: %s\nresult: %s\n",sql_full_tab_name,the_finish_query,len(sql_vals_pk),sql_vals_pk,end_pk_row)
			}
			// ------------------------------------------------------------------
			// log.Printf("table %s pk interval : %s -> %s\n",sql_full_tab_name,start_pk_row , end_pk_row)
			// log.Printf("%s\n",reflect.DeepEqual(start_pk_row,end_pk_row))
		}
		chunk_id++
		var a_chunk tablechunk
		a_chunk.table_id=j
		a_chunk.chunk_id=chunk_id
		a_chunk.begin_val=start_pk_row
		a_chunk.end_val=end_pk_row
		chunk2read <-  a_chunk
		// --------------------------------------------------------------------------
		j++
	}
	log.Printf("tableChunkBrowser finish\n")
}

// ------------------------------------------------------------------------------------------
func tableChunkReader(chunk2read chan tablechunk ,  adbConn  sql.Conn , tableInfos []MetadataTable  , id int , dumpfiletemplate string , insert_size int ) {
	log.Printf("tableChunkReader[%d] start\n",id)

	var last_tableid int = -1
	a_chunk := <- chunk2read
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
	var fh *os.File
	var err error
	a_row := make([]any,0)
	a_quote_info:= make([]bool,0)
	a_sql_row := make([]*sql.NullString,1)
	ptrs := make([]interface{},1)
	for i, _ := range a_sql_row {
		ptrs[i] = &a_sql_row[i]
	}
	file_is_empty:= make([]bool,0)
	for _ , v := range tableInfos {
		fh,err = os.OpenFile(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(dumpfiletemplate,"%d",v.dbName),"%t",v.tbName),"%p",strconv.Itoa(id)),os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if fh != nil {
			fh.Close()
			fh=nil
		}
		file_is_empty = append(file_is_empty,true)
	}
	for true {
		if last_tableid != a_chunk.table_id {
			last_tableid=a_chunk.table_id
			sql_cond_lower_pk , sql_val_indices_pk =  generatePredicat ( tableInfos[last_tableid].primaryKey , true )
			sql_cond_upper_pk , sql_val_indices_pk =  generatePredicat ( tableInfos[last_tableid].primaryKey , false )
			sql_cond_equal_pk , sql_val_indices_equal_pk =  generateEqualityPredicat ( tableInfos[last_tableid].primaryKey )
			sql_full_tab_name = fmt.Sprintf("`%s`.`%s`",tableInfos[last_tableid].dbName,tableInfos[last_tableid].tbName)
			sql_pk_cols = fmt.Sprintf("`%s`",tableInfos[last_tableid].primaryKey[0])
			cpk:=1
			for cpk < len(tableInfos[last_tableid].primaryKey) {
				sql_pk_cols = sql_pk_cols + "," + fmt.Sprintf("`%s`",tableInfos[last_tableid].primaryKey[cpk])
				cpk++
			}
			sql_tab_cols = fmt.Sprintf("`%s`",tableInfos[last_tableid].columnInfos[0].colName)
			sql_val_cols = "\"%s\""
			ctab:=1
			for ctab < len(tableInfos[last_tableid].columnInfos) {
				sql_tab_cols = sql_tab_cols + "," + fmt.Sprintf("`%s`",tableInfos[last_tableid].columnInfos[ctab].colName)
				sql_val_cols = sql_val_cols + "," + "%s"
				ctab++
			}
			the_reader_interval_query = fmt.Sprintf("select %s from %s where ( %s ) and ( %s) order by %s ",sql_tab_cols,sql_full_tab_name,sql_cond_lower_pk,sql_cond_upper_pk,sql_pk_cols )
			the_reader_equality_query = fmt.Sprintf("select %s from %s where ( %s ) order by %s ",sql_tab_cols,sql_full_tab_name,sql_cond_equal_pk,sql_pk_cols )
			a_sql_row = make([]*sql.NullString,ctab)
			ptrs = make([]interface{},ctab)
			a_quote_info= make([]bool,ctab)
			for i, _ := range a_sql_row {
				ptrs[i] = &a_sql_row[i]
				a_quote_info[i]=tableInfos[last_tableid].columnInfos[i].mustBeQuote
			}
			a_row = make([]any, ctab )
			if fh != nil {
				fh.Close()
			}
			fh,err = os.OpenFile(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(dumpfiletemplate,"%d",tableInfos[last_tableid].dbName),"%t",tableInfos[last_tableid].tbName),"%p",strconv.Itoa(id)),os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal(err)
			}
			if ( file_is_empty[last_tableid] ) {
				fh.WriteString("SET NAMES utf8mb4;\n")
				fh.WriteString("SET TIME_ZONE='+00:00';\n")
				file_is_empty[last_tableid]=false
			}
		}
		// --------------------------------------------------------------------------
		// log.Printf("[%02d] table %03d chunk %06d %s -> %s\n",id,a_chunk.table_id,a_chunk.chunk_id,a_chunk.begin_val,a_chunk.end_val)
		// --------------------------------------------------------------------------
		var sql_vals_pk []any
		var the_query *string
		if reflect.DeepEqual(a_chunk.begin_val,a_chunk.end_val) {
			sql_vals_pk = generateValuesForPredicat ( sql_val_indices_equal_pk , a_chunk.begin_val )
			the_query = &the_reader_equality_query
		} else {
			sql_vals_pk = generateValuesForPredicat ( sql_val_indices_pk , a_chunk.begin_val )
			sql_vals_pk = append(sql_vals_pk,generateValuesForPredicat ( sql_val_indices_pk , a_chunk.end_val )...)
			the_query = &the_reader_interval_query
		}
		ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
		q_rows , q_err := adbConn.QueryContext(ctx,*the_query,sql_vals_pk...)
		if q_err != nil {
			log.Printf("table %s chunk id: %d chunk query :  %s \n with %d params\nval for query: %s\n",sql_full_tab_name,a_chunk.chunk_id,*the_query,len(sql_vals_pk),sql_vals_pk)
			log.Fatalf("can not query table %s to read the chunks\n%s",sql_full_tab_name,sql_vals_pk,q_err.Error())
		}
		row_cnt:=0
		query_row_count:=0
		for q_rows.Next() {
			err := q_rows.Scan(ptrs...)
			if err != nil {
				log.Fatal(err)
			}
			row_cnt++
			query_row_count++
			// ------------------------------------------------------------------
			for n , value := range a_sql_row {
				if value == nil {
					a_row[n]="null"
				} else {
					if a_quote_info[n] {
						a_row[n]=fmt.Sprintf("\"%s\"",value.String)
					} else {
						a_row[n]= value.String
					}
				}
			}
			// ------------------------------------------------------------------
			if row_cnt == 1 {
				_, err := fh.WriteString(fmt.Sprintf("insert into `%s`(%s) values ", tableInfos[last_tableid].tbName , sql_tab_cols ))
				if err != nil {
					log.Fatal(err)
				}
				fh.WriteString(fmt.Sprintf("("+sql_val_cols+")",a_row...))
			} else {
				fh.WriteString(fmt.Sprintf(",("+sql_val_cols+")",a_row...))
			}
			// ------------------------------------------------------------------
			if row_cnt > insert_size {
				fh.WriteString(";\n")
				row_cnt=0 
			}
		}
		if row_cnt > 0 {
			fh.WriteString(";\n")
		}
		if ( mode_debug ) {
			log.Printf("table %s chunk query :  %s \n with %d params\nval for query: %s\nrows cnt: %d\n",sql_full_tab_name,*the_query,len(sql_vals_pk),sql_vals_pk,row_cnt)
		}
		// --------------------------------------------------------------------------
		a_chunk = <- chunk2read
	}
	log.Printf("tableChunkReader[%d] finish\n",id)
}



// ------------------------------------------------------------------------------------------

type arrayFlags []string

func (i *arrayFlags) String() string {
	var str_out string ;
	for key , value := range *i {
		if ( key != 0) {
			str_out = str_out + " " + value
		} else {
			str_out = value
		}
	}
	return str_out ;
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

// ------------------------------------------------------------------------------------------
func main() {
	log.SetFlags(log.Ldate|log.Lmicroseconds )
	// ----------------------------------------------------------------------------------
	var arg_debug        = flag.Bool("debug"      , false                , "debug mode")
	var arg_db_port      = flag.Int("port"        , 3306                 , "the database port")
	var arg_db_host      = flag.String("host"     ,"127.0.0.1"           , "the database host")
	var arg_db_user      = flag.String("user"     , "mysql"              , "the database connection user")
	var arg_db_pasw      = flag.String("pwd"      , ""                   , "the database connection password")
	var arg_db_parr      = flag.Int("parallel"    , 10                   , "number of workers")
	var arg_chunk_size   = flag.Int("chunksize"   , 40                   , "rows count when reading")
	var arg_insert_size  = flag.Int("insertsize"  , 10                   , "rows count for each insert")	
	var arg_lck_db       = flag.String("lockdb"   , "test"               , "the lock for sync database name")
	var arg_lck_tb       = flag.String("locktb"   , "paradumplock"       , "the lock for sync table name")
	var arg_dumpfile     = flag.String("dumpfile" , "dump_%d_%t_%p.sql"  , "sql dump of tables")
	var arg_db           = flag.String("db"       , ""                   , "database of tables to dump")
	// ------------
	var tables2dump arrayFlags ;
	flag.Var(&tables2dump,"table", "table to dump")
	flag.Parse()
	// ----------------------------------------------------------------------------------
	if tables2dump == nil {
		flag.Usage()
		return
	}
	if len(*arg_db) == 0 {
		flag.Usage()
		return
	}
	if len(*arg_dumpfile) == 0 {
		flag.Usage()
		return
	}
	if *arg_insert_size > *arg_chunk_size {
		flag.Usage()
		return
	}
	mode_debug=*arg_debug
	// ----------------------------------------------------------------------------------
	log.Print(tables2dump)
	conDb , posDb , _ := GetaSynchronizedConnections ( *arg_db_host , *arg_db_port , *arg_db_user , *arg_db_pasw , *arg_db_parr , *arg_lck_tb , *arg_lck_db )
	res_check := CheckSessions ( conDb , posDb )
	log.Printf("CheckSessions => %s", res_check)
	// ----------------------------------------------------------------------------------
	r , _ := GetMetadataInfo4Tables ( conDb[0] , *arg_db ,  tables2dump )
	log.Printf("tables infos  => %s", r)
	// ----------------------------------------------------------------------------------
	res_check = CheckSessions ( conDb , posDb )
	log.Printf("CheckSessions => %s", res_check)
	// ----------------------------------------------------------------------------------
	var wg sync.WaitGroup
	pk_chunks_to_read := make(chan tablechunk, 1000)
	wg.Add(1)
	go func() {
            defer wg.Done()
            tableChunkBrowser( conDb[0] ,  r , pk_chunks_to_read , *arg_chunk_size )
        }()
	j := 1
	for j <len(conDb)  {
		wg.Add(1)
		go func(adbConn sql.Conn ,id int ) {
			defer wg.Done()
			tableChunkReader( pk_chunks_to_read ,  adbConn ,  r , id , *arg_dumpfile , *arg_insert_size )
		}(conDb[j],j)
		j++
		time.Sleep( 5 * time.Millisecond)
	}
	wg.Wait()
	// ----------------------------------------------------------------------------------
	res_check = CheckSessions ( conDb , posDb )
	log.Printf("CheckSessions => %s", res_check)
	// ----------------------------------------------------------------------------------
}
// ------------------------------------------------------------------------------------------
