// ------------------------------------------------------------------------------------------  
package main

import (
//	"time"
//	"io/ioutil"
//	"strconv"
	"fmt"
	"log"
	"flag"
	"database/sql"
	"database/sql/driver"
	_ "github.com/go-sql-driver/mysql"
)
/* ------------------------------------------------------------------------------------------  
   ParaDump is a tool that will create a dump of mysql in table , by using multiple threads
   to read tables .

   ------------------------------------------------------------------------------------------ */

/* ------------------------------------------------------------------------------------------
   
   http://go-database-sql.org/importing.html

   https://pkg.go.dev/flag@go1.19.4
   ------------------------------------------------------------------------------------------ */

// ------------------------------------------------------------------------------------------
func GetaSynchronizedConnections( DbHost string , DbPort int , DbUsername string , DbUserPassword string , _TargetCount int , _LockTable string  , LockDatabase string ) ([30]*driver.Conn, error) {

	db , err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",DbUsername,DbUserPassword,DbHost,DbPort,LockDatabase))
	if err != nil {
		log.Fatal(err)
	}
	for i:=0; i<30; i++ {
		
	}
}
	
/*	
    lckqry = "lock tables test.resynctablelock READ"
    cursor.execute(lckqry)
    lckqry = "START TRANSACTION WITH CONSISTENT SNAPSHOT"
	

	    qry = "SHOW STATUS LIKE 'binlog_snapshot_%'"
    cursor.execute(qry)
    r = {}
    for l in cursor:
        if l["Variable_name"] == "Binlog_snapshot_file":
            r["file"] = l["Value"]
        if l["Variable_name"] == "Binlog_snapshot_position":
            r["position"] = int(l["Value"])
    cursor.close()
    cursor = GetCnxCursor(cnx)



	
}


func GetArray(arr [5]int) [5]int {
	arr[2] = 100
	return arr
}
db, err := sql.Open("mysql", "user:password@/dbname")
if err != nil {
	panic(err)
}


// See "Important settings" section.
db.SetConnMaxLifetime(time.Minute * 3)
db.SetMaxOpenConns(10)
db.SetMaxIdleConns(10)

*/


// ------------------------------------------------------------------------------------------  
func main() {
	// ----------------------------------------------------------------------------------
	var arg_db_port = flag.Int("port"     , 3306           , "the database port")
	var arg_db_host = flag.String("host"  ,"127.0.0.1"     , "the database host")
	var arg_db_user = flag.String("user"  , "mysql"        , "the database connection user")
	var arg_db_pasw = flag.String("pwd"   , ""             , "the database connection password")
	var arg_lck_db  = flag.String("lockdb", "test"         , "the lock for sync database name")
	var arg_lck_tb  = flag.String("locktb", "paradumplock" , "the lock for sync table name")
	// ----------------------------------------------------------------------------------
	flag.Parse()
	_ , _ = GetaSynchronizedConnections ( *arg_db_host , *arg_db_port , *arg_db_user , *arg_db_pasw , 10 , *arg_lck_tb , *arg_lck_db )
}
// ------------------------------------------------------------------------------------------  


