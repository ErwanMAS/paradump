#!/bin/bash

cd "$(dirname "$0")"

BINARY=../src/paradump
DB_HOST="127.0.0.1"
DB_PORTS="4000 5000"
DCK_MYSQL="$NEED_SUDO docker run --network=host -i mysql/mysql-server:8.0.31  /usr/bin/mysql"

while [[ -n "$1" ]]
do
    if [[ "$1" = "--debug" ]]
    then
	set -x
	set -e
	shift
	continue
    fi
    if [[ "$1" = "--binary" && -n "$2" ]]
    then
	BINARY="$2"
	shift 2
	continue
    fi
    if [[ "$1" = "--dbhost" && -n "$2" ]]
    then
	DB_HOST="$2"
	shift 2
	continue
    fi
    if [[ "$1" = "--dbports" && -n "$2" ]]
    then
	DB_PORTS="$2"
	shift 2
	continue
    fi
    if [[ "$1" = "--local-mysql" ]]
    then
	DCK_MYSQL="mysql"
	shift 1
	continue
    fi
    if [[ -n "$1" ]]
    then
	echo "ERROR unkon arg '$1'"
	exit 1
    fi
done

if [[ ! -x "$BINARY" ]]
then
    echo "ERROR '$BINARY' is not executable"
    exit 2
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

TMPDIR=$(mktemp -d )
for port in $DB_PORTS
do
    echo "timing mysqldump $port on ${DB_HOST}"
    time bash -c "${DCK_MYSQL}dump  -u root -ptest1234  --port $port -h ${DB_HOST}  --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --column-statistics=0 foobar > /dev/null"
    echo "timing mysqlpump $port on ${DB_HOST}"
    time bash -c "${DCK_MYSQL}pump  -u root -ptest1234  --port $port -h ${DB_HOST}  --skip-add-drop-table --skip-add-locks   --no-create-info --no-create-db          --databases foobar > /dev/null"
    echo "timing paradump sql $port on ${DB_HOST}"
    time bash -c "$BINARY  -port $port -host ${DB_HOST} -pwd test1234 -user foobar  -guessprimarykey -db foobar -alltables --dumpmode sql -dumpfile ${TMPDIR}/dump_%d_%t_%p.%m" 
    echo "timing paradump csv $port on ${DB_HOST}"
    time bash -c "$BINARY  -port $port -host ${DB_HOST} -pwd test1234 -user foobar  -guessprimarykey -db foobar -alltables --dumpmode csv -dumpfile ${TMPDIR}/dump_%d_%t_%p.%m"
done

if [ -d "$TMPDIR" ]
then
    rm -rf "${TMPDIR}"
fi
