#!/bin/bash

cd $(dirname $0)

docker ps -a -q >/dev/null 2>&1 || {
    echo can not connect to docker
    exit 1
}

SRC_DB="percona:ps-5.6.51=mysql_source=1"
TGT_DB="mysql/mysql-server:8.0.31=mysql_target=2"

for V in $SRC_DB=4000 $TGT_DB=5000
do
    IMG=$( echo "$V"| cut -d= -f1)
    NAM=$( echo "$V"| cut -d= -f2)
    SID=$( echo "$V"| cut -d= -f3)
    PRT=$( echo "$V"| cut -d= -f4)

    docker run --name ${NAM} -p ${PRT}:3306 -e MYSQL_ROOT_PASSWORD=test1234 -e MYSQL_DATABASE=foobar -e MYSQL_USER=foobar -eMYSQL_PASSWORD=test1234 -d ${IMG} mysqld  --server-id=${SID} --log-bin=/var/lib/mysql/mysql-bin.log  --binlog-format=ROW
done

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
    docker exec -i $NAM  sh -c "/usr/bin/mysql  -u root -ptest1234  -h localhost mysql -e \"create database test ; GRANT ALL PRIVILEGES ON test.* TO 'foobar'@'%'; create table test.paradumplock ( val_int int , val_str varchar(256) ) ENGINE=INNODB ; \"  "
    for F in create_tab_*.sql
    do
	cat $F | docker exec -i $NAM  sh -c '/usr/bin/mysql  -u foobar -ptest1234  -h 127.0.0.1 foobar '
    done
done
for D in dump_*.sql.zstd
do
    zstd -dc ${D} | docker exec -i  mysql_source  sh -c '/usr/bin/mysql  -u foobar -ptest1234  -h 127.0.0.1 foobar '
done
