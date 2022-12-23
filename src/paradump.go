// ------------------------------------------------------------------------------------------
package main

import (
	"time"
//	"io/ioutil"
	"strconv"
	"context"
	"fmt"
	"log"
	"flag"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)
/* ------------------------------------------------------------------------------------------
   ParaDump is a tool that will create a dump of mysql in table , by using multiple threads
   to read tables .

   ------------------------------------------------------------------------------------------ */

/* ------------------------------------------------------------------------------------------

   http://go-database-sql.org/importing.html

   https://gobyexample.com/

   https://pkg.go.dev/fmt#Sprintf

   https://pkg.go.dev/database/sql

   https://go.dev/tour/moretypes/15    ( slice )

   https://pkg.go.dev/flag@go1.19.4
   ------------------------------------------------------------------------------------------ */

// ------------------------------------------------------------------------------------------
func LockTableWaitRelease( jobdone chan bool , conn *sql.Conn , LockTable string  , LockDatabase string ) {
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
	log.Print("going to sleep for X secs")
	time.Sleep( 3 * time.Second)
	log.Print("done sleeping")

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	_ , r_err := conn.ExecContext(ctx,"UNLOCK TABLES;")
	if r_err != nil {
		log.Fatalf("can not unlock tabless\n%s\n",r_err.Error())
	}
	jobdone <-true
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
	db.SetMaxOpenConns(30)
	db.SetMaxIdleConns(30)
	// --------------------
	var ctx context.Context
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	// --------------------
	var db_conns [30] *sql.Conn
	for i:=0; i<30; i++ {
		first_conn , err := db.Conn(ctx)
		if err != nil {
			log.Fatal(err)
		}
		db_conns[i] = first_conn
	}
	// --------------------
	tablelockchan := make(chan bool)
	startsesschan := make(chan InfoSqlSession, 29)
	go LockTableWaitRelease(tablelockchan,db_conns[0],LockTable,LockDatabase)
	time.Sleep( 1 * time.Second)
	for i:=1; i<30; i++ {
		go LockTableStartConsistenRead(startsesschan,i,db_conns[i],LockTable,LockDatabase)
	}
	oklock := <- tablelockchan
	if ( oklock ) {
		log.Print("ok for master lock and release")
	}
	var stats_ses [] StatSqlSession
	var db_sessions_filepos [29] InfoSqlSession
	for i:=1; i<30; i++ {
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
		for i:=0; i<29; i++ {
			if db_sessions_filepos[i].FileName ==  stats_ses[foundRefPos].FileName && db_sessions_filepos[i].FilePos ==  stats_ses[foundRefPos].FilePos  && len(ret_dbconns) < TargetCount {
				ret_dbconns=append(ret_dbconns ,*db_conns[db_sessions_filepos[i].cnxId])
				db_conns[db_sessions_filepos[i].cnxId]=nil
			}
		}
	}
	for i:=0; i<30; i++ {
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
}

type indexInfo struct {
	column       []string
	cardinality  int64
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
		result.columnInfos = append ( result.columnInfos , a_col)
	}

	/*
		for allrows.Next() {
			var v_name string

	*/
			//        select TABLE_ROWS  from information_schema.tables WHERE table_name = %s    AND table_schema = %s
			//	select COLUMN_NAME , DATA_TYPE,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS where table_name=%s and  TABLE_SCHEMA=%s "


	//	select COLUMN_NAME  from INFORMATION_SCHEMA.STATISTICS WHERE table_name = %s    AND table_schema = %s and INDEX_NAME = 'PRIMARY' order by SEQ_IN_INDEX
	//      select COLUMN_NAME,CARDINALITY,INDEX_NAME,SEQ_IN_INDEX  from INFORMATION_SCHEMA.STATISTICS WHERE  table_schema = 'foobar' and table_name = 'client_activity' and INDEX_NAME != 'PRIMARY' order by INDEX_NAME,SEQ_IN_INDEX ;


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

// ------------------------------------------------------------------------------------------



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
	var arg_db_port = flag.Int("port"     , 3306           , "the database port")
	var arg_db_host = flag.String("host"  ,"127.0.0.1"     , "the database host")
	var arg_db_user = flag.String("user"  , "mysql"        , "the database connection user")
	var arg_db_pasw = flag.String("pwd"   , ""             , "the database connection password")
	var arg_lck_db  = flag.String("lockdb", "test"         , "the lock for sync database name")
	var arg_lck_tb  = flag.String("locktb", "paradumplock" , "the lock for sync table name")
	var arg_db      = flag.String("db"    , ""             , "database of tables to dump")
	// ------------
	var tables2dump arrayFlags ;
	flag.Var(&tables2dump,"table", "table to dump")
	// ----------------------------------------------------------------------------------
	flag.Parse()
	if tables2dump == nil {
		flag.Usage()
		return
	}
	if len(*arg_db) == 0 {
		flag.Usage()
		return
	}
	// ----------------------------------------------------------------------------------
	log.Print(tables2dump)
	conDb , posDb , _ := GetaSynchronizedConnections ( *arg_db_host , *arg_db_port , *arg_db_user , *arg_db_pasw , 10 , *arg_lck_tb , *arg_lck_db )
	res_check := CheckSessions ( conDb , posDb )
	log.Printf("CheckSessions => %s", res_check)
	// ----------------------------------------------------------------------------------
	r , _ := GetMetadataInfo4Tables ( conDb[0] , *arg_db ,  tables2dump )
	log.Printf("tables infos  => %s", r)
	// ----------------------------------------------------------------------------------
	res_check = CheckSessions ( conDb , posDb )
	log.Printf("CheckSessions => %s", res_check)
}
// ------------------------------------------------------------------------------------------
//
// state   1 2 3 4
//
// transition  1 - 10
// transition  1 - 3
// transition  3 - 4
// transition  3 - 11
// transition  2 - 4
// transition  10 - 2
// transition  11 - 2
//

//
// 1

// 1 - 10 - 2
// 1 - 3  - 11 - 2

// 1 - 3

// 1 - 10 - 2 - 4
// 1 - 3  - 4
// 1 - 3  - 11 - 2 - 4
