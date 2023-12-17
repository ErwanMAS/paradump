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
#------------------------------------------------------------------------------------------
SRC_DB="percona:ps-5.6.51=mysql_source=1"
TGT_DB="mysql/mysql-server:8.0.31=mysql_target=2"
SRC_PG="postgres:10.23-bullseye=postgres_source=3"
TGT_PG="postgres:13.10-bullseye=postgres_target=4"
SRC_MS="mcr.microsoft.com/mssql/server:2019-latest=mssql_source=5"
TGT_MS="mcr.microsoft.com/mssql/server:2022-latest=mssql_target=6"


ALL_INSTANCES="$SRC_DB $TGT_DB"
ALL_INSTANCES_PORT="$SRC_DB=4000 $TGT_DB=4900"

ALL_INSTANCES="$SRC_MS $TGT_MS"
ALL_INSTANCES_PORT="$SRC_MS=8200 $TGT_MS=8300"

ALL_INSTANCES="$SRC_MS $TGT_MS $SRC_DB $TGT_DB $SRC_PG $TGT_PG"
ALL_INSTANCES_PORT="$SRC_MS=8200 $TGT_MS=8300 $SRC_PG=8000 $TGT_PG=8100 $SRC_DB=4000 $TGT_DB=4900"
#------------------------------------------------------------------------------------------
BUILD_DCK=1
LOAD_DATA=1
#
if [[ "$1" = "--load-data" ]]
then
    BUILD_DCK=0
fi    
if [[ "$1" = "--check-data" ]]
then
    BUILD_DCK=0
    LOAD_DATA=0
fi
#--------------------
PAT_FILES="init_ticket_tag.sql.zst"
PAT_FILES="init_*.sql.zst"
#------------------------------------------------------------------------------------------
if [[ "$BUILD_DCK" = 1 ]]
then   
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
    #----------------------------------------------------------------------------------------------------
    if [[ "$(uname -s)" != "Darwin"  ]]
    then
	MEM_GB=$(( ( $( sed 's/^MemTotal:  *\([0-9]*\) kB$/\1/p;d' /proc/meminfo ) / 1024 ) / 1024 ))
    else
	MEM_GB=$(( ( ( $( sysctl hw.memsize | sed 's/^hw.memsize: \([0-9][0-9]*\)$/\1/' ) / 1024 ) / 1024 ) / 1024 ))
    fi

    MYSQL_BUF="18"
    if [[ "$MEM_GB" -le 64 ]]
    then
	MYSQL_BUF="12"
    fi
    if [[ "$MEM_GB" -le 32 ]]
    then
	MYSQL_BUF="6"
    fi
    if [[ "$MEM_GB" -le 16 ]]
    then
	MYSQL_BUF="3"
    fi
    MSSQL_MEMORY_LIMIT_MB=$(( MYSQL_BUF * 1024 ))
    MYSQL_BUF="${MYSQL_BUF}G"
    #----------------------------------------------------------------------------------------------------
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
	if [[ "$SFT" = "mssql" ]]
	then
	    LOCAL_PORT=1433
	fi
	DCK_NET="--publish=$PRT:${LOCAL_PORT}"
	if [[ "$SFT" = "mysql" ]]
	then
	    $NEED_SUDO docker run --platform="${DOCKER_ARCH}"   "$DCK_NET" --name "${NAM}" -e MYSQL_ROOT_PASSWORD=Test+12345 -e MYSQL_DATABASE=foobar -e MYSQL_USER=foobar -eMYSQL_PASSWORD=Test+12345 -d "${IMG}" mysqld  --server-id="${SID}"  \
			   --log-bin=/var/lib/mysql/mysql-bin.log  --binlog-format=ROW --innodb_buffer_pool_size="${MYSQL_BUF}" --max_connections=600 "${EXTRA_PARAMS[@]}" --innodb_buffer_pool_instances=8                                  ||
		$NEED_SUDO docker run --platform=linux/amd64    "$DCK_NET" --name "${NAM}" -e MYSQL_ROOT_PASSWORD=Test+12345 -e MYSQL_DATABASE=foobar -e MYSQL_USER=foobar -eMYSQL_PASSWORD=Test+12345 -d "${IMG}" mysqld  --server-id="${SID}"  \
			   --log-bin=/var/lib/mysql/mysql-bin.log  --binlog-format=ROW --innodb_buffer_pool_size="${MYSQL_BUF}" --max_connections=600 "${EXTRA_PARAMS[@]}" --innodb_buffer_pool_instances=8
	fi
	if [[ "$SFT" = "postgres" ]]
	then
	    $NEED_SUDO docker run --platform="${DOCKER_ARCH}"    "$DCK_NET" --name "${NAM}" -e POSTGRES_PASSWORD=Test+12345 -e POSTGRES_DB=paradump -e POSTGRES_USER=admin -d "${IMG}" ||
		$NEED_SUDO docker run --platform=linux/amd64     "$DCK_NET" --name "${NAM}" -e POSTGRES_PASSWORD=Test+12345 -e POSTGRES_DB=paradump -e POSTGRES_USER=admin -d "${IMG}"
	fi
	if [[ "$SFT" = "mssql" ]]
	then
	    $NEED_SUDO docker run --platform="${DOCKER_ARCH}"    "$DCK_NET" --name "${NAM}" -e MSSQL_MEMORY_LIMIT_MB="$MSSQL_MEMORY_LIMIT_MB" -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Test+12345" -d "${IMG}" ||
		$NEED_SUDO docker run --platform=linux/amd64     "$DCK_NET" --name "${NAM}" -e MSSQL_MEMORY_LIMIT_MB="$MSSQL_MEMORY_LIMIT_MB" -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Test+12345" -d "${IMG}"
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
		    READY_DB=$( $NEED_SUDO docker exec -i "${NAM}"  mysqladmin -h localhost -u root -pTest+12345 ping 2>&1 | grep -c 'mysqld is alive' )
		fi
		if [[ "$SFT" = "postgres" ]]
		then
		    READY_DB=$( $NEED_SUDO docker exec -i "${NAM}"  pg_isready  -h localhost -U admin -d paradump >/dev/null 2>&1 && echo 1 || echo 0 )
		fi
		if [[ "$SFT" = "mssql" ]]
		then
		    READY_DB=$( $NEED_SUDO docker exec -i "${NAM}"  /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P 'Test+12345' -Q "select 'OK_CONNECTED' " 2>/dev/null | grep -c OK_CONNECTED )
		fi
		sleep 2
		T=$(( T - 1 ))
	    done
	    C=$(( C -1 ))
	done
	printf "container %s listen on %s is READY\n" "$NAM" "$PRT"
	if [[ "$SFT" = "mysql" ]]
	then
	    $NEED_SUDO docker exec  -e MYSQL_PWD=Test+12345 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"create database test   ; GRANT ALL PRIVILEGES ON test.*   TO 'foobar'@'%'; \"  "
	    $NEED_SUDO docker exec  -e MYSQL_PWD=Test+12345 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"create database barfoo ; GRANT ALL PRIVILEGES ON barfoo.* TO 'foobar'@'%'; \"  "
	    [ "$PRT" -ne 4000 ] && $NEED_SUDO docker exec  -e MYSQL_PWD=Test+12345 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"create user 'root'@'%' identified by 'Test+12345' ; \"  "
	    $NEED_SUDO docker exec  -e MYSQL_PWD=Test+12345 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"GRANT all privileges on *.* TO 'root'@'%' ; \"  "
	    $NEED_SUDO docker exec  -e MYSQL_PWD=Test+12345 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"GRANT RELOAD on *.* TO 'foobar'@'%' ; \"  "
	    $NEED_SUDO docker exec  -e MYSQL_PWD=Test+12345 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"GRANT REPLICATION CLIENT on *.* TO 'foobar'@'%' ; \"  "
	    $NEED_SUDO docker exec  -e MYSQL_PWD=Test+12345 -i "${NAM}"  sh -c "/usr/bin/mysql  -u root -h localhost mysql -e \"set global innodb_stats_persistent_sample_pages = 2048000 ; \"  "
	fi
	if [[ "$SFT" = "postgres" ]]
	then
	    for DB in foobar barfoo test
	    do
		$NEED_SUDO docker exec -i "${NAM}"  psql  -q -h localhost -U admin -d paradump -c " create schema $DB ; "
	    done
	fi
	if [[ "$SFT" = "mssql" ]]
	then
	    (
		cat <<-EOF
		create database paradump
		GO
		create login admin with password = 'Test+12345'
		GO
		USE paradump
		GO
		create user admin for login admin
		GO
		GRANT  CREATE FUNCTION, CREATE PROCEDURE, CREATE RULE, CREATE TABLE, CREATE VIEW TO admin
		GO
		CREATE SCHEMA foobar AUTHORIZATION admin
		GO
		CREATE SCHEMA barfoo AUTHORIZATION admin
		GO
		CREATE SCHEMA test AUTHORIZATION admin
		GO
		EOF
	    ) | $NEED_SUDO docker exec -i "${NAM}"  /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P 'Test+12345'
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
				$NEED_SUDO docker exec  -e MYSQL_PWD=Test+12345 -i "${NAM}"  sh -c '/usr/bin/mysql  -u foobar -h localhost '${DB} < "$F"
			    fi
			    if [[ "$SFT" = "postgres" ]]
			    then
				$NEED_SUDO docker exec  -e PGOPTIONS="-c search_path=$DB" -i "${NAM}"   psql -q  -h localhost -U admin -d paradump  < "$F"
			    fi
			    if [[ "$SFT" = "mssql" ]]
			    then
				export DB
				{
				    echo "cat <<EOF" ;
				    cat "$F"
				    echo "GO"
				    echo EOF
				} | bash | $NEED_SUDO docker exec -i "${NAM}"  /opt/mssql-tools/bin/sqlcmd -S localhost -d paradump -U admin -P 'Test+12345'
			    fi
			) 2>&1 | tail -100
		    fi
		done
	    done
	    wait
	done
    done
fi
#------------------------------------------------------------------------------------------
if [[ "$LOAD_DATA" = 1 ]]
then   
    echo "loading data"
    for ENGINE in mysql postgres mssql
    do
	for DB in foobar barfoo
	do
	    if [[ $DB = "barfoo" ]]
	    then
		CONTAINER_DST=${ENGINE}_target
	    else
		CONTAINER_DST=${ENGINE}_source
	    fi
	    if [ "$( $NEED_SUDO docker ps -q -fName=${CONTAINER_DST} | wc -l )" -eq 0 ]
	    then
		continue
	    fi
	    if [[ "$ENGINE" = "mysql" ]]
	    then
		ENV_CMD="MYSQL_PWD=Test+12345"
		CNT_EXE="/usr/bin/mysql  -u foobar  -h localhost ${DB} "
	    fi
	    if [[ "$ENGINE" = "postgres" ]]
	    then
		ENV_CMD="PGOPTIONS=-c search_path=$DB"
		CNT_EXE="psql -q  -h localhost -U admin -d paradump"
	    fi
	    if [[ "$ENGINE" = "mssql" ]]
	    then
		ENV_CMD="FOOBAR=foobar"
		CNT_EXE="/opt/mssql-tools/bin/sqlcmd -S localhost -d paradump -U admin -P Test+12345"
	    fi
	    for D in $PAT_FILES
	    do
		(
		    # shellcheck disable=SC2001
		    TAB=$( echo "$D" | sed 's|^init_\(.*\).sql.zst|\1|p;d' )
		    if [[ "$BUILD_DCK" = 0 ]]
		    then
			echo "TRUNCATING table $TAB in DB $DB for $ENGINE"
			# shellcheck disable=SC2086
			( echo "truncate table ${DB}.$TAB ;" | $NEED_SUDO docker exec  -e "$ENV_CMD" -i  $CONTAINER_DST $CNT_EXE )
		    fi
		    echo "loading  $D ( aka table $TAB ) in DB $DB for $ENGINE"
		    echo "[../..]"
		    (
			if [[ "$ENGINE" = "postgres" ]]
			then
			    if [[ $TAB = "ticket_tag" ]]
			    then
				# shellcheck disable=SC2086,SC2016,SC2030
				( time zstd -dc "${D}" | ( export LC_ALL=C ; sed 's/INSERT INTO `\([^`]*\)`/INSERT INTO \1/' | sed 's|\\"|"|g;s|\\Z|\\x1A|g;s|\\0||g;s|\(,[0-9][0-9]*,\)|\1E|g;' ) | $NEED_SUDO docker exec  -e "$ENV_CMD" -i  $CONTAINER_DST $CNT_EXE  ) 2>&1
			    else
				if [[ $TAB = "account_metadatas" ]]
				then
				    # shellcheck disable=SC2086,SC2016,SC2030,SC2031
				    ( time zstd -dc "${D}" | ( export LC_ALL=C ; sed 's/INSERT INTO `\([^`]*\)`/INSERT INTO \1/' | sed "s|,0x\([0-9A-F][0-9A-F]*\),|,decode('\1','hex'),|g" ) |                       $NEED_SUDO docker exec  -e "$ENV_CMD" -i  $CONTAINER_DST $CNT_EXE  ) 2>&1
				else
				    # shellcheck disable=SC2086,SC2016,SC2030,SC2031
				    ( time zstd -dc "${D}" | ( export LC_ALL=C ; sed 's/INSERT INTO `\([^`]*\)`/INSERT INTO \1/' ) |                                                                                  $NEED_SUDO docker exec  -e "$ENV_CMD" -i  $CONTAINER_DST $CNT_EXE  ) 2>&1
				fi
			    fi
			fi
			if [[ "$ENGINE" = "mssql" ]]
			then
			    if [[ $TAB = "ticket_tag" ]]
			    then
				# shellcheck disable=SC2086,SC2016,SC2030,SC2031
				( time zstd -dc "${D}" | ( export LC_ALL=C ; sed 's/INSERT INTO `\([^`]*\)`/GO\nINSERT INTO '$DB'.\1/;s/INSERT INTO \([^\. ]*\) VA/GO\nINSERT INTO '$DB'.\1 VA/;s|\(,[0-9][0-9]*,\)|\1N|g;s|\\"|"|g;' |
							                     sed 's/\\0'"/'+CHAR(0)+'/;"'s/\\n'"/'+CHAR(10)+'/;"'s/\\r'"/'+CHAR(13)+'/;"'s/\\Z'"/'+CHAR(26)+'/;"'s|\\\\|\\|' | sed "s|\\\\'',|''',|g;" | sed 's/),(/),\n(/g' )               | $NEED_SUDO docker exec  -i  $CONTAINER_DST $CNT_EXE  ) 2>&1
			    else
				# shellcheck disable=SC2086,SC2016,SC2031,SC2030
				if [[ $TAB = "account_metadatas" ]]
				then
				    ( time zstd -dc "${D}" | ( export LC_ALL=C ; sed 's/INSERT INTO `\([^`]*\)`/GO\nINSERT INTO '$DB'.\1/;s/INSERT INTO \([^\. ]*\) VA/GO\nINSERT INTO '$DB'.\1 VA/' | sed 's/),(/),\n(/g'   | sed "s|,0x\([0-9A-F][0-9A-F]*\),|,sys.fn_cdc_hexstrtobin('0x\1'),|g" ) | $NEED_SUDO docker exec  -i  $CONTAINER_DST $CNT_EXE  ) 2>&1
				else
				    ( time zstd -dc "${D}" | ( export LC_ALL=C ; sed 's/INSERT INTO `\([^`]*\)`/GO\nINSERT INTO '$DB'.\1/;s/INSERT INTO \([^\. ]*\) VA/GO\nINSERT INTO '$DB'.\1 VA/' | sed 's/),(/),\n(/g' ) |                                                                         $NEED_SUDO docker exec  -i  $CONTAINER_DST $CNT_EXE  ) 2>&1
				fi
			    fi
			fi
			if [[ "$ENGINE" = "mysql" ]]
			then
			    # shellcheck disable=SC2086
			    ( time zstd -dc "${D}" | $NEED_SUDO docker exec  -e "$ENV_CMD" -i  $CONTAINER_DST $CNT_EXE  ) 2>&1
			fi
			echo "done load  $D ( aka table $TAB ) in DB $DB for $ENGINE" ; echo ; echo ; echo 
		    ) | tail -15
		) | tail -20 &
		if [[ "$ENGINE" = "mssql" ]]
		then
		    CNT_JR=$(jobs -l >/dev/null 2>&1 ; (jobs -lr | wc -l))
		    while [ "$CNT_JR" -gt 3 ]
		    do
			sleep 20
			CNT_JR=$(jobs -l >/dev/null 2>&1 ; (jobs -lr | wc -l))
		    done
		fi
	    done
	done
    done
    wait
    echo "optimize table"
    for DB in foobar barfoo
    do
	if [[ $DB = "barfoo" ]]
	then
	    CONTAINER_DST=mysql_target
	else
	    CONTAINER_DST=mysql_source
	fi
	if [ "$( $NEED_SUDO docker ps -q -fName=${CONTAINER_DST} | wc -l )" -eq 0 ]
	then
	    continue
	fi
	for D in $PAT_FILES
	do
	    (
		T=$(echo "$D" | cut -d_ -f2- | cut -d. -f1)
		echo "optimize  $T on DB $DB"
		( time $NEED_SUDO docker exec  -e MYSQL_PWD=Test+12345 -i  $CONTAINER_DST  sh -c "/usr/bin/mysql  -u foobar  -h localhost ${DB} -e 'optimize table $T;' " ) 2>&1
	    ) | tail -100 &
	done
    done
    wait
    echo "converting barfoo on mysql_source"
    DB=barfoo
    CONTAINER_DST=mysql_source
    if [ "$( $NEED_SUDO docker ps -q -fName=${CONTAINER_DST} | wc -l )" -eq 1 ]
    then
	for D in $PAT_FILES
	do
	    (
		T=$(echo "$D" | cut -d_ -f2- | cut -d. -f1)
		echo "changing charset for $T on DB $DB on mysql_source"
		$NEED_SUDO docker exec  -e MYSQL_PWD=Test+12345 -i  $CONTAINER_DST  sh -c "/usr/bin/mysql  -u foobar  -h localhost ${DB} -e 'alter table $T CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;' " 2>&1
		echo "done for $T"
	    )	| tail -100 &
	done
	wait
    fi
    echo "done"
fi
#------------------------------------------------------------------------------------------
CMP_FILE=$(mktemp)
(
cat <<-EOF
CHECK_COUNT=account_metadatas=REFERENCE=foobar=360
CHECK_COUNT=client_activity=REFERENCE=foobar=203220
CHECK_COUNT=client_info=REFERENCE=foobar=120
CHECK_COUNT=location_history=REFERENCE=foobar=378300
CHECK_COUNT=sensor_info=REFERENCE=foobar=1605360
CHECK_COUNT=sensor_tag=REFERENCE=foobar=60
CHECK_COUNT=sensors_pairing=REFERENCE=foobar=1605360
CHECK_COUNT=text_notifications=REFERENCE=foobar=60
CHECK_COUNT=ticket_history=REFERENCE=foobar=680760
CHECK_COUNT=ticket_tag=REFERENCE=foobar=60
EOF
for ENGINE in mysql postgres mssql
do
    for DB in foobar barfoo
    do
	if [[ $DB = "barfoo" ]]
   	then
	    CONTAINER_DST=${ENGINE}_target
	else
	    CONTAINER_DST=${ENGINE}_source
	fi
	if [ "$( $NEED_SUDO docker ps -q -fName=${CONTAINER_DST} | wc -l )" -eq 0 ]
	then
	    continue
	fi
	if [[ "$ENGINE" = "mysql" ]]
	then
	    ENV_CMD="MYSQL_PWD=Test+12345"
	    CNT_EXE="/usr/bin/mysql  -u foobar  -h localhost ${DB} "
	fi
	if [[ "$ENGINE" = "postgres" ]]
	then
	    ENV_CMD="PGOPTIONS=-c search_path=$DB"
	    CNT_EXE="psql -qAt -F, -h localhost -U admin -d paradump"
	fi
	if [[ "$ENGINE" = "mssql" ]]
	then
	    ENV_CMD="FOOBAR=foobar"
	    CNT_EXE="/opt/mssql-tools/bin/sqlcmd -S localhost -d paradump -U admin -P Test+12345"
	fi
	for D in init_*.sql.zst
	do
	    # shellcheck disable=SC2001
	    TAB=$( echo "$D" | sed 's|^init_\(.*\).sql.zst|\1|p;d' )
	    # shellcheck disable=SC2086
	    ( ( echo "select 'CHECK_COUNT = 'as REM , '$TAB' as T , '=' as A , '$ENGINE' as ENGINE , '=' as B , '$DB' as D , '=' as C , count(*) as CNT from $DB.$TAB;" | $NEED_SUDO docker exec  -e "$ENV_CMD" -i  $CONTAINER_DST $CNT_EXE  ) 2>&1 | tail -500 ) & 
	done
    done
done
wait
) | sed 's|,||g;s|\t||g;s| ||g;' | grep ^CHEC | sort >> "$CMP_FILE"
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
echo "COUNT DATA in $CMP_FILE"
#
CMP_FILE=$(mktemp)
DBG_FILE=$(mktemp) 
(
for ENGINE in mysql postgres mssql
do
    for DB in foobar barfoo
    do
	if [[ $DB = "barfoo" ]]
   	then
	    CONTAINER_DST=${ENGINE}_target
	else
	    CONTAINER_DST=${ENGINE}_source
	fi
	if [ "$( $NEED_SUDO docker ps -q -fName=${CONTAINER_DST} | wc -l )" -eq 0 ]
	then
	    continue
	fi
	if [[ "$ENGINE" = "mysql" ]]
	then
	    ENV_CMD="MYSQL_PWD=Test+12345"
	    CNT_EXE="/usr/bin/mysql  -u foobar  -h localhost ${DB} "
	fi
	if [[ "$ENGINE" = "postgres" ]]
	then
	    ENV_CMD="PGOPTIONS=-c search_path=$DB"
	    CNT_EXE="psql -qAt -F, -h localhost -U admin -d paradump"
	fi
	if [[ "$ENGINE" = "mssql" ]]
	then
	    ENV_CMD="FOOBAR=foobar"
	    CNT_EXE="/opt/mssql-tools/bin/sqlcmd -S localhost -d paradump -U admin -P Test+12345"
	fi
	for D in mysql:ticket_tag:"label_hex_u8 <> hex(cast(convert(label using utf8mb4)  as binary))" \
		 mysql:ticket_tag:"label_hex_u16 <> hex(cast(convert(label using utf16)  as binary))" \
		 mysql:ticket_tag:"label_hex_l1 <> hex(cast(convert(label using latin1 )  as binary))" \
		 mysql:sensor_tag:"cast( hex(label) as char character set latin1 ) <>  hex_label"   \
		 postgres:ticket_tag:"upper(encode(convert_to(label,'UTF8'),'hex')) <> label_postgres_hex_u8" \
		 mssql:ticket_tag:"convert(varchar(max),CAST(label as varbinary(256)),2) <> label_hex_u16le"
	do
	    ENG=$( echo "$D"  | cut -d: -f1 )
	    if [[ "$ENG" == "$ENGINE" ]]
	    then
		TAB=$( echo "$D"  | cut -d: -f2 )
		WHR=$( echo "$D"  | cut -d: -f3- )
		# shellcheck disable=SC2086
		( ( echo "select 'CHECK_COUNT = 'as REM , '$TAB' as T , '=' as A , '$ENGINE' as ENGINE , '=' as B , '$DB' as D , '=' as C , count(*) as CNT from $DB.$TAB where $WHR ;" | $NEED_SUDO docker exec  -e "$ENV_CMD" -i  $CONTAINER_DST $CNT_EXE  ) 2>&1 | tail -500 ) &
	    fi
	done
    done
done
wait
) | tee "$DBG_FILE" | sed 's|,||g;s|\t||g;s| ||g;' | grep ^CHEC | sort > "$CMP_FILE"
#
if [[ $( grep -v -c '=0$' "$CMP_FILE" ) != 0 ]]
then
    echo "BUG import CMP_FILE $CMP_FILE DBG_FILE $DBG_FILE"
    grep -v '=0$' "$CMP_FILE"
fi    
echo "CHECK DATA in $CMP_FILE"
#
