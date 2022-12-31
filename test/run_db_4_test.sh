#!/bin/bash

cd $(dirname $0)

docker ps -a -q >/dev/null 2>&1 || {
    echo can not connect to docker
    exit 1
}

SRC_DB="percona:ps-5.6.51=mysql_source=1"
TGT_DB="mysql/mysql-server:8.0.31=mysql_target=2"

for V in $SRC_DB $TGT_DB
do
    NAM=$( echo "$V"| cut -d= -f2)

    IS_RUNNING=$(docker ps -q --filter name=${NAM})

    if [ -n "$IS_RUNNING" ]
    then
	echo some docker image is already running
	exit 1
    fi
done
echo creating docker database instances

for V in $SRC_DB=4000 $TGT_DB=5000
do
    IMG=$( echo "$V"| cut -d= -f1)
    NAM=$( echo "$V"| cut -d= -f2)
    SID=$( echo "$V"| cut -d= -f3)
    PRT=$( echo "$V"| cut -d= -f4)

    docker run --name ${NAM} -p ${PRT}:3306 -e MYSQL_ROOT_PASSWORD=test1234 -e MYSQL_DATABASE=foobar -e MYSQL_USER=foobar -eMYSQL_PASSWORD=test1234 -d ${IMG} mysqld  --server-id=${SID} --log-bin=/var/lib/mysql/mysql-bin.log  --binlog-format=ROW
done
echo creating database objects
for V in $SRC_DB=4000 $TGT_DB=5000
do
    IMG=$( echo "$V"| cut -d= -f1)
    NAM=$( echo "$V"| cut -d= -f2)
    SID=$( echo "$V"| cut -d= -f3)
    PRT=$( echo "$V"| cut -d= -f4)

    T=30
    while [ $( docker exec -i $NAM  mysqladmin -h localhost -u root -ptest1234 ping 2>&1 | grep -c 'mysqld is alive' ) -eq 0 -a $T -gt 0 ]
    do
        sleep 2
	T=$(( $T - 1 ))
    done
    docker exec  -e MYSQL_PWD=test1234 -i $NAM  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"create database test ; GRANT ALL PRIVILEGES ON test.* TO 'foobar'@'%'; create table test.paradumplock ( val_int int , val_str varchar(256) ) ENGINE=INNODB ; \"  "
    for DB in foobar test
    do
	for F in create_tab_*.sql
	do
	    cat $F | docker exec  -e MYSQL_PWD=test1234 -i $NAM  sh -c '/usr/bin/mysql  -u foobar -h 127.0.0.1 '${DB}
	done
    done
done
echo loading data
for D in dump_*.sql.zstd
do
    ( zstd -dc ${D} | docker exec  -e MYSQL_PWD=test1234 -i  mysql_source  sh -c '/usr/bin/mysql  -u foobar  -h 127.0.0.1 foobar ' ) &
done
wait
echo done

#
#  truncate test.client_info ; truncate test.client_activity ; truncate test.ticket_history ;
#
#  use test ; source dump_foobar_client_info_1.sql ; source dump_foobar_client_info_2.sql ; source dump_foobar_client_info_3.sql ;
#  select count(*) from test.client_info ; select count(*) from foobar.client_info ; select count(*) from ( select * from test.client_info union select * from foobar.client_info ) e ;
#
#  use test ; source dump_foobar_client_activity_1.sql ; source dump_foobar_client_activity_2.sql ; source dump_foobar_client_activity_3.sql ;
#  select count(*) from test.client_activity ; select count(*) from foobar.client_activity ; select count(*) from ( select * from test.client_activity union select * from foobar.client_activity ) e ;
#
#  use test ; source dump_foobar_ticket_history_1.sql ; source dump_foobar_ticket_history_2.sql ; source dump_foobar_ticket_history_3.sql ;
#  select count(*) from test.ticket_history ; select count(*) from foobar.ticket_history ; select count(*) from ( select * from test.ticket_history union select * from foobar.ticket_history ) e ;
#  
#  select count(*) from client_info ;
#  select count(*) from (select distinct clientid from client_activity ) e ;
#
#  select count(*) from (select distinct ticketid from client_activity ) e ;
#  select count(*) from (select distinct ticketid from ticket_history ) e ;
#
#  select count(*) from text_notifications ;
#
#  select count(*) from mail_queue ;
#
