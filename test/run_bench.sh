#!/bin/bash

cd "$(dirname $0)"

BINARY=../src/paradump

DEBUG_CMD=">/dev/null 2>&1"

if [ "$1" = "--debug" ]
then
    set -x
    set -e
    DEBUG_CMD=""
fi

NEED_SUDO=""
docker ps -a -q >/dev/null 2>&1 || {
    echo can not connect to docker
    echo trying with sudo
    sudo docker ps -a -q >/dev/null 2>&1 || {
	echo ERROR can not connect to docker with or without sudo
	exit 1
    }
    NEED_SUDO="sudo"
}

DCK_MYSQL="$NEED_SUDO docker run --network=host -i mysql/mysql-server:8.0.31  /usr/bin/mysql"

TMPDIR=$(mktemp -d )
for port in 5000 4000
do
    echo timing mysqldump
    time bash -c "${DCK_MYSQL}dump  -u root -ptest1234  --port $port -h 127.0.0.1  --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --column-statistics=0 foobar > $TMPDIR/mysqldump"
    echo timing mysqlpump
    time bash -c "${DCK_MYSQL}pump  -u root -ptest1234  --port $port -h 127.0.0.1  --skip-add-drop-table --skip-add-locks   --no-create-info --no-create-db          --databases foobar > $TMPDIR/mysqlpump" 
    echo timing paradump sql
    time bash -c "$BINARY  -port $port -pwd test1234 -user foobar  -guessprimarykey -db foobar -alltables --dumpmode sql -dumpfile ${TMPDIR}/dump_%d_%t_%p.%m" 
    echo timing paradump csv
    time bash -c "$BINARY  -port $port -pwd test1234 -user foobar  -guessprimarykey -db foobar -alltables --dumpmode csv -dumpfile ${TMPDIR}/dump_%d_%t_%p.%m"
done

