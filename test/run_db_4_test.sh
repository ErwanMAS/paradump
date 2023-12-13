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
SRC_PG="postgres:10.23-bullseye=postgres_source=3"
TGT_PG="postgres:13.10-bullseye=postgres_target=4"

ALL_INSTANCES="$SRC_DB $TGT_DB $SRC_PG $TGT_PG"

ALL_INSTANCES_PORT="$SRC_PG=8000 $TGT_PG=8900 $SRC_DB=4000 $TGT_DB=4900"

CNT_ERR=0
for V in $ALL_INSTANCES
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

MYSQL_BUF="18G"
if [[ "$MEM_GB" -le 64 ]]
then
    MYSQL_BUF="12G"
fi
if [[ "$MEM_GB" -le 32 ]]
then
    MYSQL_BUF="6G"
fi
if [[ "$MEM_GB" -le 16 ]]
then
    MYSQL_BUF="3G"
fi
DOCKER_ARCH=$( $NEED_SUDO docker version -f json| jq -jr '.Server|(.Os,"/",.Arch)' )

printf "creating docker database instances ( default docker arch is %s )\n" "${DOCKER_ARCH}"

declare -a EXTRA_PARAMS

for V in $ALL_INSTANCES_PORT
do
    IMG=$( echo "$V"| cut -d= -f1)
    NAM=$( echo "$V"| cut -d= -f2)
    SID=$( echo "$V"| cut -d= -f3)
    PRT=$( echo "$V"| cut -d= -f4)

    SFT=$( echo "$NAM"|cut -d_ -f1)
    if [[ "$SFT" = "mysql" ]]
    then
	if [[ "$PRT" -eq 4000 ]]
	then
	    EXTRA_PARAMS[0]="--innodb_adaptive_hash_index_partitions=8"
	else
	    EXTRA_PARAMS[0]="--innodb_adaptive_hash_index_parts=8"
	    EXTRA_PARAMS[1]="--default-time-zone=America/New_York"
	fi
        LOCAL_PORT=3306
    fi
    if [[ "$SFT" = "postgres" ]]
    then
	LOCAL_PORT=5432
    fi
    if [[ "$(uname -s)" != "Darwin"  ]]
    then
	DCK_NET="--net=host"
	EXTRA_PARAMS+=("--port=${PRT}")
    else
	DCK_NET="--publish=$PRT:${LOCAL_PORT}"
    fi
    if [[ "$SFT" = "mysql" ]]
    then
	$NEED_SUDO docker run --platform="${DOCKER_ARCH}"   "$DCK_NET" --name "${NAM}" -e MYSQL_ROOT_PASSWORD=test1234 -e MYSQL_DATABASE=foobar -e MYSQL_USER=foobar -eMYSQL_PASSWORD=test1234 -d "${IMG}" mysqld  --server-id="${SID}"  \
		       --log-bin=/var/lib/mysql/mysql-bin.log  --binlog-format=ROW --innodb_buffer_pool_size="${MYSQL_BUF}" --max_connections=600 "${EXTRA_PARAMS[@]}" --innodb_buffer_pool_instances=8                                  ||
	    $NEED_SUDO docker run --platform=linux/amd64    "$DCK_NET" --name "${NAM}" -e MYSQL_ROOT_PASSWORD=test1234 -e MYSQL_DATABASE=foobar -e MYSQL_USER=foobar -eMYSQL_PASSWORD=test1234 -d "${IMG}" mysqld  --server-id="${SID}"  \
		       --log-bin=/var/lib/mysql/mysql-bin.log  --binlog-format=ROW --innodb_buffer_pool_size="${MYSQL_BUF}" --max_connections=600 "${EXTRA_PARAMS[@]}" --innodb_buffer_pool_instances=8
    fi
    if [[ "$SFT" = "postgres" ]]
    then
	$NEED_SUDO docker run --platform="${DOCKER_ARCH}"    "$DCK_NET" --name "${NAM}" -e POSTGRES_PASSWORD=test1234 -e POSTGRES_DB=paradump -e POSTGRES_USER=admin -d "${IMG}" ||
	    $NEED_SUDO docker run --platform=linux/amd64     "$DCK_NET" --name "${NAM}" -e POSTGRES_PASSWORD=test1234 -e POSTGRES_DB=paradump -e POSTGRES_USER=admin -d "${IMG}"
    fi
done
echo creating database objects
for V in $ALL_INSTANCES_PORT
do
    IMG=$( echo "$V"| cut -d= -f1)
    NAM=$( echo "$V"| cut -d= -f2)
    SID=$( echo "$V"| cut -d= -f3)
    PRT=$( echo "$V"| cut -d= -f4)

    SFT=$( echo "$NAM"|cut -d_ -f1)
    
    T=60
    C=3
    while [[ $C -gt 0 ]]
    do
	READY_DB=0
	while [[ "${READY_DB}" -eq 0 && "$T" -gt 0 ]]
	do
	    if [[ "$SFT" = "mysql" ]]
	    then
		READY_DB=$( $NEED_SUDO docker exec -i "${NAM}"  mysqladmin -h localhost -u root -ptest1234 ping 2>&1 | grep -c 'mysqld is alive' )
	    fi
	    if [[ "$SFT" = "postgres" ]]
	    then
		READY_DB=$( $NEED_SUDO docker exec -i "${NAM}"  pg_isready  -h localhost -U admin -d paradump >/dev/null 2>&1 && echo 1 || echo 0 )
	    fi
	    sleep 2
	    T=$(( T - 1 ))
	done
	C=$(( C -1 ))
    done
    printf "container %s listen on %s is READY\n" "$NAM" "$PRT"
    if [[ "$SFT" = "mysql" ]]
    then
	$NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"create database test   ; GRANT ALL PRIVILEGES ON test.*   TO 'foobar'@'%'; \"  "
	$NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"create database barfoo ; GRANT ALL PRIVILEGES ON barfoo.* TO 'foobar'@'%'; \"  "
	[ "$PRT" -ne 4000 ] && $NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"create user 'root'@'%' identified by 'test1234' ; \"  "
	$NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"GRANT all privileges on *.* TO 'root'@'%' ; \"  "
	$NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"GRANT RELOAD on *.* TO 'foobar'@'%' ; \"  "
	$NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"GRANT REPLICATION CLIENT on *.* TO 'foobar'@'%' ; \"  "
	$NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"set global innodb_stats_persistent_sample_pages = 2048000 ; \"  "
    fi
    if [[ "$SFT" = "postgres" ]]
    then
	for DB in foobar barfoo test
	do
	    $NEED_SUDO docker exec -i "${NAM}"  psql  -q -h localhost -U admin -d paradump -c " create schema $DB ; "
	done
    fi
    for KIND in tab viw
    do
	for DB in foobar barfoo test
	do
	    for F in "${SFT}/create_${KIND}_"*.sql
	    do
		if [ -f "$F" ]
		then
		    (
			echo "run $F in $DB"
			if [[ "$SFT" = "mysql" ]]
			then
			    $NEED_SUDO docker exec  -e MYSQL_PWD=test1234 -i "${NAM}"  sh -c '/usr/bin/mysql  -u foobar -h localhost '${DB} < "$F"
			fi
			if [[ "$SFT" = "postgres" ]]
			then
			    $NEED_SUDO docker exec  -e PGOPTIONS="-c search_path=$DB" -i "${NAM}"   psql -q  -h localhost -U admin -d paradump  < "$F"
			fi
		    ) 2>&1 | tail -100 &
		fi
	    done
	done
	wait
    done
done
echo "loading data"
for ENGINE in mysql postgres
do
    for DB in foobar barfoo
    do
	if [[ "$ENGINE" = "mysql" ]]
	then
	    ENV_CMD="MYSQL_PWD=test1234"
	    CNT_EXE="/usr/bin/mysql  -u foobar  -h localhost ${DB} "
	fi
	if [[ "$ENGINE" = "postgres" ]]
	then
	    ENV_CMD="PGOPTIONS=-c search_path=$DB"
	    CNT_EXE="psql -q  -h localhost -U admin -d paradump"
	fi
	for D in init_*.sql.zst
	do
	    # shellcheck disable=SC2001
	    TAB=$( echo "$D" | sed 's|^init_\(.*\).sql.zst|\1|p;d' )
	    (
		echo "loading  $D ( aka table $TAB ) in DB $DB for $ENGINE"
		if [[ $DB = "barfoo" ]]
		then
		    CONTAINER_DST=${ENGINE}_target
		else
		    CONTAINER_DST=${ENGINE}_source
		fi
		if [[ $TAB = "ticket_tag" && "$ENGINE" = "postgres" ]]
		then
		    # shellcheck disable=SC2086,SC2016,SC2030
		    ( time zstd -dc "${D}" | ( export LC_ALL=C ; sed 's/INSERT INTO `\([^`]*\)`/INSERT INTO \1/' | sed 's|\\"|"|g;s|\\Z|\\x1A|g;s|\\0||g;s|\(,[0-9][0-9]*,\)|\1E|g;' ) | $NEED_SUDO docker exec  -e "$ENV_CMD" -i  $CONTAINER_DST $CNT_EXE  ) 2>&1
		else
		    if [[ $TAB = "account_metadatas" && "$ENGINE" = "postgres" ]]
		    then
			# shellcheck disable=SC2086,SC2016,SC2030,SC2031
			( time zstd -dc "${D}" | ( export LC_ALL=C ; sed 's/INSERT INTO `\([^`]*\)`/INSERT INTO \1/' | sed "s|,0x\([0-9A-F][0-9A-F]*\),|,decode('\1','hex'),|g" ) |                       $NEED_SUDO docker exec  -e "$ENV_CMD" -i  $CONTAINER_DST $CNT_EXE  ) 2>&1
		    else
			# shellcheck disable=SC2086,SC2016,SC2031
			( time zstd -dc "${D}" | ( export LC_ALL=C ; sed 's/INSERT INTO `\([^`]*\)`/INSERT INTO \1/' ) |                                                                                  $NEED_SUDO docker exec  -e "$ENV_CMD" -i  $CONTAINER_DST $CNT_EXE  ) 2>&1
		    fi
		fi
	    ) | tail -500 &
	done
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
#
CMP_FILE=$(mktemp) 
(
for ENGINE in mysql postgres
do
    for DB in foobar barfoo
    do
	if [[ "$ENGINE" = "mysql" ]]
	then
	    ENV_CMD="MYSQL_PWD=test1234"
	    CNT_EXE="/usr/bin/mysql  -u foobar  -h localhost ${DB} "
	fi
	if [[ "$ENGINE" = "postgres" ]]
	then
	    ENV_CMD="PGOPTIONS=-c search_path=$DB"
	    CNT_EXE="psql -qAt -F, -h localhost -U admin -d paradump"
	fi
	if [[ $DB = "barfoo" ]]
   	then
	    CONTAINER_DST=${ENGINE}_target
	else
	    CONTAINER_DST=${ENGINE}_source
	fi
	for D in init_*.sql.zst
	do
	    # shellcheck disable=SC2001
	    TAB=$( echo "$D" | sed 's|^init_\(.*\).sql.zst|\1|p;d' )
	    # shellcheck disable=SC2086
	    ( ( echo "select 'CHECK_COUNT = 'as REM , '$TAB' as T , '=' as A , '$ENGINE' as ENGINE , '=' as B , '$DB' as D , '=' as C , count(*) as CNT from $TAB;" | docker exec  -e "$ENV_CMD" -i  $CONTAINER_DST $CNT_EXE  ) 2>&1 | tail -500 ) & 
	done
    done
done
wait
) | sed 's|,||g;s|\t||g;s| ||g;' | grep ^CHEC | sort > "$CMP_FILE"
#
O_T=""
O_C="-1"
while read -r L
do
    T=$( echo "$L" | cut -d= -f 2)
    C=$( echo "$L" | cut -d= -f 5)
    if [[ "$T" == "$O_T"  ]]
    then
	if [[ $C -ne $O_C ]]
	then
	    echo BUG import with "$L"
	fi
    else
	O_T=$T
	O_C=$C
    fi
done < "$CMP_FILE"
#
(
for ENGINE in mysql postgres
do
    for DB in foobar barfoo
    do
	if [[ "$ENGINE" = "mysql" ]]
	then
	    ENV_CMD="MYSQL_PWD=test1234"
	    CNT_EXE="/usr/bin/mysql  -u foobar  -h localhost ${DB} "
	fi
	if [[ "$ENGINE" = "postgres" ]]
	then
	    ENV_CMD="PGOPTIONS=-c search_path=$DB"
	    CNT_EXE="psql -qAt -F, -h localhost -U admin -d paradump"
	fi
	if [[ $DB = "barfoo" ]]
   	then
	    CONTAINER_DST=${ENGINE}_target
	else
	    CONTAINER_DST=${ENGINE}_source
	fi
	for D in mysql:ticket_tag:"label_hex_u8 <> hex(cast(convert(label using utf8mb4)  as binary))" \
		 mysql:ticket_tag:"label_hex_l1 <> hex(cast(convert(label using latin1 )  as binary))" \
		 mysql:sensor_tag:"cast( hex(label) as char character set latin1 ) <>  hex_label"   \
		 postgres:ticket_tag:"upper(encode(convert_to(label,'UTF8'),'hex')) <> label_postgres_hex_u8"
	do
	    ENG=$( echo "$D"  | cut -d: -f1 )
	    if [[ "$ENG" == "$ENGINE" ]]
	    then
		TAB=$( echo "$D"  | cut -d: -f2 )
		WHR=$( echo "$D"  | cut -d: -f3- )
		# shellcheck disable=SC2086
		( ( echo "select 'CHECK_COUNT = 'as REM , '$TAB' as T , '=' as A , '$ENGINE' as ENGINE , '=' as B , '$DB' as D , '=' as C , count(*) as CNT from $TAB where $WHR ;" | docker exec  -e "$ENV_CMD" -i  $CONTAINER_DST $CNT_EXE  ) 2>&1 | tail -500 ) &
	    fi
	done
    done
done
wait
) | sed 's|,||g;s|\t||g;s| ||g;' | grep ^CHEC | sort > "$CMP_FILE"
#
if [[ $( grep -c '=0$' "$CMP_FILE" ) != 0 ]]
then
    echo BUG import
    grep '=0$' "$CMP_FILE"
fi    
#
