#!/bin/bash

cd "$(dirname "$0")" || exit 1

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

SRC_DB="percona:ps-5.6.51=mysql_source=1"
TGT_DB="mysql/mysql-server:8.0.31=mysql_target=2"

CNT_ERR=0
for V in $SRC_DB $TGT_DB
do
    NAM=$( echo "$V"| cut -d= -f2)

    IS_RUNNING=$($NEED_SUDO docker ps -q -a --filter name="${NAM}")

    if [[ -n "$IS_RUNNING" ]]
    then
	echo ERROR some docker image is already running
	$NEED_SUDO docker ps -a --filter name="${NAM}"
	CNT_ERR=$(( CNT_ERR + 1 ))
	echo
    fi
done
if [[ $CNT_ERR -gt 0 ]]
then
    exit 1
fi

if [[ "$(uname -s)" != "Darwin"  ]]
then
    MEM_GB=$(( ( $( sed 's/^MemTotal:  *\([0-9]*\) kB$/\1/p;d' /proc/meminfo ) / 1024 ) / 1024 ))
else
    MEM_GB=$(( ( ( $( sysctl hw.memsize | sed 's/^hw.memsize: \([0-9][0-9]*\)$/\1/' ) / 1024 ) / 1024 ) / 1024 ))
fi

MYSQL_BUF="25G"
if [[ "$MEM_GB" -le 64 ]]
then
    MYSQL_BUF="15G"
fi
if [[ "$MEM_GB" -le 32 ]]
then
    MYSQL_BUF="7G"
fi
if [[ "$MEM_GB" -le 16 ]]
then
    MYSQL_BUF="3G"
fi
DOCKER_ARCH=$( $NEED_SUDO docker version -f json| jq -jr '.Server|(.Os,"/",.Arch)' )

printf "creating docker database instances ( default docker arch is %s )\n" "${DOCKER_ARCH}"

declare -a EXTRA_PARAMS

for V in $SRC_DB=4000 $TGT_DB=4900
do
    IMG=$( echo "$V"| cut -d= -f1)
    NAM=$( echo "$V"| cut -d= -f2)
    SID=$( echo "$V"| cut -d= -f3)
    PRT=$( echo "$V"| cut -d= -f4)

    if [[ "$PRT" -eq 4000 ]]
    then
	EXTRA_PARAMS[0]="--innodb_adaptive_hash_index_partitions=8"
    else
	EXTRA_PARAMS[0]="--innodb_adaptive_hash_index_parts=8"
	EXTRA_PARAMS[1]="--default-time-zone=America/New_York"
    fi
    if [[ "$(uname -s)" != "Darwin"  ]]
    then
	DCK_NET="--net=host"
	EXTRA_PARAMS+=("--port=${PRT}")
    else
	DCK_NET="--publish=$PRT:3306"
    fi
    $NEED_SUDO docker run --platform="${DOCKER_ARCH}"   "$DCK_NET" --name "${NAM}" -e MYSQL_ROOT_PASSWORD=test1234 -e MYSQL_DATABASE=foobar -e MYSQL_USER=foobar -eMYSQL_PASSWORD=test1234 -d "${IMG}" mysqld  --server-id="${SID}"  \
	       --log-bin=/var/lib/mysql/mysql-bin.log  --binlog-format=ROW --innodb_buffer_pool_size="${MYSQL_BUF}" --max_connections=600 "${EXTRA_PARAMS[@]}" --innodb_buffer_pool_instances=8                                  ||
	$NEED_SUDO docker run --platform=linux/amd64 "$DCK_NET" --name "${NAM}" -e MYSQL_ROOT_PASSWORD=test1234 -e MYSQL_DATABASE=foobar -e MYSQL_USER=foobar -eMYSQL_PASSWORD=test1234 -d "${IMG}" mysqld  --server-id="${SID}"  \
	       --log-bin=/var/lib/mysql/mysql-bin.log  --binlog-format=ROW --innodb_buffer_pool_size="${MYSQL_BUF}" --max_connections=600 "${EXTRA_PARAMS[@]}" --innodb_buffer_pool_instances=8
done
echo creating database objects
for V in $SRC_DB=4000 $TGT_DB=4900
do
    IMG=$( echo "$V"| cut -d= -f1)
    NAM=$( echo "$V"| cut -d= -f2)
    SID=$( echo "$V"| cut -d= -f3)
    PRT=$( echo "$V"| cut -d= -f4)

    T=60
    C=3
    while [[ $C -gt 0 ]]
    do
	while [[ "$( $NEED_SUDO docker exec -i "${NAM}"  mysqladmin -h localhost -u root -ptest1234 ping 2>&1 | grep -c 'mysqld is alive' )" -eq 0 && "$T" -gt 0 ]]
	do
	    sleep 2
	    T=$(( T - 1 ))
	done
	C=$(( C -1 ))
    done
    $NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"create database test   ; GRANT ALL PRIVILEGES ON test.*   TO 'foobar'@'%'; create table test.paradumplock ( val_int int , val_str varchar(256) ) ENGINE=INNODB ; \"  "
    $NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"create database barfoo ; GRANT ALL PRIVILEGES ON barfoo.* TO 'foobar'@'%'; \"  "
    [ "$PRT" -ne 4000 ] && $NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"create user 'root'@'%' identified by 'test1234' ; \"  "
    $NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"GRANT all privileges on *.* TO 'root'@'%' ; \"  "
    $NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"GRANT RELOAD on *.* TO 'foobar'@'%' ; \"  "
    $NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"GRANT REPLICATION CLIENT on *.* TO 'foobar'@'%' ; \"  "
    $NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"set global innodb_stats_persistent_sample_pages = 2048000 ; \"  "
    for DB in foobar barfoo test
    do
	for F in create_tab_*.sql
	do
	    (
		echo "run $F in $DB"
		$NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c '/usr/bin/mysql  -u foobar -h localhost '${DB} < "$F"
	    ) | tail -100 &
	done
    done
    wait
    for DB in foobar barfoo test
    do
	for F in create_viw_*.sql
	do
	    (
		echo "run $F in $DB"
		$NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c '/usr/bin/mysql  -u foobar -h localhost '${DB} < "$F"
	    ) | tail -100 &
	done
    done
    wait
done
echo "loading data"
for DB in foobar barfoo
do
    for D in init_*.sql.zst
    do
	(
	    echo "loading  $D  on DB $DB"
	    if [[ $DB = "barfoo" ]]
	    then
		CONTAINER_DST=mysql_target
	    else
		CONTAINER_DST=mysql_source
	    fi
	    ( time zstd -dc "${D}" | $NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i  $CONTAINER_DST sh -c "/usr/bin/mysql  -u foobar  -h localhost ${DB} "  ) 2>&1
	) | tail -100 &
    done
done
wait
echo "optimize table"
for DB in foobar barfoo
do
    for D in init_*.sql.zst
    do
	(
	    T=$(echo "$D" | cut -d_ -f2- | cut -d. -f1)
	    echo "optimize  $T on DB $DB"
	    if [[ $DB = "barfoo" ]]
	    then
		CONTAINER_DST=mysql_target
	    else
		CONTAINER_DST=mysql_source
	    fi
	    ( time $NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i  $CONTAINER_DST  sh -c "/usr/bin/mysql  -u foobar  -h localhost ${DB} -e 'optimize table $T;' " ) 2>&1
	) | tail -100 &
    done
done
wait
echo "converting barfoo on mysql_source"
DB=barfoo
CONTAINER_DST=mysql_source
for D in init_*.sql.zst
do
    (
        T=$(echo "$D" | cut -d_ -f2- | cut -d. -f1)
        echo "changing charset for $T on DB $DB on mysql_source"
	$NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i  $CONTAINER_DST  sh -c "/usr/bin/mysql  -u foobar  -h localhost ${DB} -e 'alter table $T CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;' " 2>&1
	echo "done for $T"
    )	| tail -100 &
done
wait
echo "done"

