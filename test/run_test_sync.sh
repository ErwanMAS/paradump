#!/bin/bash

cd "$(dirname "$0")" || exit 200

exec &> >( while read -r L ; do echo "$(date '+%b %d %T')" "$L" ; done )

BINARY=../bin/parasync
# ------------------------------------------------------------------------------------------
sleep_1_sec() {
    sleep 1
}
trap sleep_1_sec EXIT
# ------------------------------------------------------------------------------------------
TESTS_21X=1
TESTS_22X=1
DEBUG_CMD=">/dev/null 2>&1"
while [ -n "$1" ]
do
	if [[ "$1" = "--debug" ]]
	then
	    set -x
	    DEBUG_CMD=""
	    shift
	    continue
	fi
	if [[ "$1" = "--tests-22x" ]]
	then
	    TESTS_21X=0
	    TESTS_22X=1
	    shift
	    continue
	fi
	echo "ERROR ARG '$1' is invalid"
	exit 203
done
NEED_SUDO=""
docker ps -a -q >/dev/null 2>&1 || {
    echo can not connect to docker
    echo trying with sudo
    sudo docker ps -a -q >/dev/null 2>&1 || {
	echo ERROR can not connect to docker with or without sudo
	exit 201
    }
    NEED_SUDO="sudo"
}
# ------------------------------------------------------------------------------------------
DCK_MYSQL="$NEED_SUDO docker run --rm --network=host -i bitnami/mysql:5.7.41  /opt/bitnami/mysql/bin/mysql -u foobar -pTest+12345 -h 127.0.0.1 "
DCK_MYSQL_DUMP="$NEED_SUDO docker run --rm --network=host -i bitnami/mysql:5.7.41  /opt/bitnami/mysql/bin/mysqldump -u foobar -pTest+12345 -h 127.0.0.1 "
DCK_PSQL="$NEED_SUDO docker run --rm --network=host -e PGPASSWORD=Test+12345 -i bitnami/postgresql:11-debian-11 psql -h 127.0.0.1 -U admin -d paradump -qAt -F: "
DCK_MSSQL="$NEED_SUDO docker run --rm --network=host      -i mcr.microsoft.com/mssql/server:2022-latest /opt/mssql-tools/bin/sqlcmd -U admin -d paradump -P Test+12345   -S 127.0.0.1"
# ------------------------------------------------------------------------------------------
# tables that have binary or line return that will prevent to use CSV
#
LIST_SET_1='ticket_tag account_metadatas'
#
# tables that are small
#
LIST_SET_2="client_info text_notifications mail_queue sensor_tag"
#
LIST_SMALL_TABLES="${LIST_SET_1} ${LIST_SET_2}"
# ------------------------------------------------------------------------------------------
echo "Compute reference count"
for port in 4000
do
    echo
    for T in $LIST_SMALL_TABLES
    do
	for DB in foobar
	do
	    CNT=$(${DCK_MYSQL} --port $port $DB -e "select count(*) as cnt from $T \G" 2>/dev/null | sed 's/^cnt: //p;d')
	    if [[ -z "$CNT" ]]
	    then
		echo "can cont count from $T"
		exit 202
	    fi
	    eval "CNT_$T=$CNT"
	    echo "port $port db $DB table $T count $CNT"
	done
    done
done
# ------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------
# test 200 do mysqldump only 
TMPDIR_T200=$(mktemp -d )

for T in $LIST_SMALL_TABLES
do
    ${DCK_MYSQL_DUMP} --port 4000 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact foobar "$T"  2>/dev/null  | grep -a -v '^$' >  "${TMPDIR_T200}/mysqldump_foobar_${T}.sql" 2>/dev/null
done
for T in $LIST_SMALL_TABLES
do
    CNT_LINES_SRC=$( eval "echo \$CNT_$T" )
    CNT_LINES_MDP=$(LANG=C grep -c '^INSERT' < "${TMPDIR_T200}/mysqldump_foobar_${T}".sql )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_MDP" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 200: failure ($FAIL)"
    echo "info are in $TMPDIR_T200"
    exit 200
fi
echo "Test 200: ok ( $? )"

# ------------------------------------------------------------------------------------------
# test 201 [mysql:mysql] copy small tables sql => count rows / foobar.* => barfoo.*
TMPDIR=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey -dst-schema barfoo -dst-port=4900 -dst-user=foobar -dst-pwd=Test+12345      $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  --statsfile ${TMPDIR}/stat_activity_table --writer-no-delete --writer-no-update --writer-no-insert  $DEBUG_CMD " || { echo "Test 201: failure" ; exit 201 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    CNT=$(${DCK_MYSQL} --port 4900 foobar -e "select count(*) as cnt from $T \G" 2>/dev/null | sed 's/^cnt: //p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
    ${DCK_MYSQL_DUMP} --port 4900 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact foobar "$T"  2>/dev/null  | grep -a -v '^$' >  "${TMPDIR}/mysqldump_foobar_${T}.sql" 2>/dev/null
done
for T in $LIST_SMALL_TABLES
do
    CNT_LINES_SRC=$(grep -c '^INSERT' "${TMPDIR_T200}/mysqldump_foobar_${T}".sql )
    CNT_LINES_DST=$(grep -c '^INSERT' "${TMPDIR}/mysqldump_foobar_${T}".sql )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_DST" ]]
    then
	FAIL=$((FAIL+1))
    else
	DIFF_RES=$(mktemp)
	diff -u <( sort "${TMPDIR_T200}/mysqldump_foobar_${T}".sql ) <( sort "${TMPDIR}/mysqldump_foobar_${T}".sql )  > "$DIFF_RES"
	CNT_PLUS=$( grep -c '^+'  "$DIFF_RES" )
	CNT_MINUS=$( grep -c '^-'  "$DIFF_RES" )
	if [[ "$CNT_PLUS" -gt 0 || "$CNT_MINUS" -gt 0 ]]
	then
	    FAIL=$((FAIL+1))
	else
	    rm "$DIFF_RES"
	fi
    fi
done
SOME_WRITE=$(grep -c -v  'noop *0 wrt *0$' ${TMPDIR}/stat_activity_table  )
if [[ "SOME_WRITE" -gt 0 ]]
then
    FAIL=$((FAIL+1))
fi    
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 201: failure ($FAIL)"
    echo "info are in $TMPDIR"
    echo "info T200 are in ${TMPDIR_T200}"
    exit 201
fi
echo "Test 201: ok ( $? )"
rm -rf "$TMPDIR"

# test 202 [mysql:mysql] copy small tables sql => count rows / barfoo.* => foobar.*
TMPDIR=$(mktemp -d )
eval "$BINARY  -dst-port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema barfoo -dst-schema foobar -guessprimarykey -port=4900 -dst-user=foobar -dst-pwd=Test+12345      $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' ) --statsfile ${TMPDIR}/stat_activity_table --writer-no-delete --writer-no-update --writer-no-insert $DEBUG_CMD " || { echo "Test 202: failure" ; exit 202 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    CNT=$(${DCK_MYSQL} --port 4900 barfoo -e "select count(*) as cnt from $T \G" 2>/dev/null | sed 's/^cnt: //p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
    ${DCK_MYSQL_DUMP} --port 4900 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact barfoo "$T"  2>/dev/null  | grep -a -v '^$' >  "${TMPDIR}/mysqldump_barfoo_${T}.sql" 2>/dev/null
done
for T in $LIST_SMALL_TABLES
do
    CNT_LINES_SRC=$(grep -c '^INSERT' "${TMPDIR_T200}/mysqldump_foobar_${T}".sql )
    CNT_LINES_DST=$(grep -c '^INSERT' "${TMPDIR}/mysqldump_barfoo_${T}".sql )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_DST" ]]
    then
	FAIL=$((FAIL+1))
    else
	DIFF_RES=$(mktemp)
	diff -u <( sort "${TMPDIR_T200}/mysqldump_foobar_${T}".sql ) <( sort "${TMPDIR}/mysqldump_barfoo_${T}".sql )  > "$DIFF_RES"
	CNT_PLUS=$( grep -c '^+'  "$DIFF_RES" )
	CNT_MINUS=$( grep -c '^-'  "$DIFF_RES" )
	if [[ "$CNT_PLUS" -gt 0 || "$CNT_MINUS" -gt 0 ]]
	then
	    FAIL=$((FAIL+1))
	else
	    rm "$DIFF_RES"
	fi
    fi
done
SOME_WRITE=$(grep -c -v  'noop *0 wrt *0$' ${TMPDIR}/stat_activity_table  )
if [[ "SOME_WRITE" -gt 0 ]]
then
    FAIL=$((FAIL+1))
fi    
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 202: failure ($FAIL)"
    echo "info are in $TMPDIR"
    echo "info T200 are in ${TMPDIR_T200}"
    exit 202
fi
echo "Test 202: ok ( $? )"
rm -rf "$TMPDIR"

# test 203 [mysql:pgsql] copy small tables sql => count rows / foobar.* => barfoo.
TMPDIR=$(mktemp -d )
eval "$BINARY -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -dst-schema barfoo -guessprimarykey -dst-port=8100 -dst-user=admin -dst-pwd=Test+12345 -dst-driver postgres -dst-db paradump  $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' ) --statsfile ${TMPDIR}/stat_activity_table --writer-no-delete --writer-no-update --writer-no-insert $DEBUG_CMD " || { echo "Test 203: failure" ; exit 203 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    CNT=$(${DCK_PSQL} --port 8100 -c "select 'cnt',count(*) as cnt from foobar.$T ;" 2>/dev/null | sed 's/^cnt *: *\([^ ]\)/\1/p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8100  -c "select 'cnt_match',count(*) as cnt_match  from foobar.ticket_tag where upper(encode(convert_to(label,'UTF8'),'hex')) = label_postgres_hex_u8 ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+32))
fi
CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8100  -c "select 'cnt_match',count(*) as cnt_match  from foobar.account_metadatas where metasha256 = encode(sha256(metavalue),'hex') ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
then
    FAIL=$((FAIL+64))
fi
SOME_WRITE=$( sed 's|^\(ticket_tag.*dst.* noop *\) 1|\1 0|' ${TMPDIR}/stat_activity_table | grep -c -v  'noop *0 wrt *0$' )
if [[ "SOME_WRITE" -gt 0 ]]
then
    FAIL=$((FAIL+1024))
fi    
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 203: failure ($FAIL)"
    echo "info are in $TMPDIR"
    exit 203    
fi
echo "Test 203: ok ( $? )"
rm -rf "$TMPDIR"

# test 204 [mysql:pgsql] copy small tables sql => count rows / barfoo.* => foobar.*
TMPDIR=$(mktemp -d )
eval "$BINARY -port 4900 -pwd Test+12345 -user foobar  -guessprimarykey -schema barfoo -dst-schema foobar -guessprimarykey -dst-port=8000 -dst-user=admin -dst-pwd=Test+12345 -dst-driver postgres -dst-db paradump  $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  --statsfile ${TMPDIR}/stat_activity_table --writer-no-delete --writer-no-update --writer-no-insert $DEBUG_CMD " || { echo "Test 204: failure" ; exit 204 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    CNT=$(${DCK_PSQL} --port 8000 -c "select 'cnt',count(*) as cnt from barfoo.$T ;" 2>/dev/null | sed 's/^cnt *: *\([^ ]\)/\1/p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8000  -c "select 'cnt_match',count(*) as cnt_match  from barfoo.ticket_tag where upper(encode(convert_to(label,'UTF8'),'hex')) = label_postgres_hex_u8 ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+32))
fi
CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8000  -c "select 'cnt_match',count(*) as cnt_match  from barfoo.account_metadatas where metasha256 = encode(sha256(metavalue),'hex') ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
then
    FAIL=$((FAIL+64))
fi
SOME_WRITE=$( sed 's|^\(ticket_tag.*dst.* noop *\) 1|\1 0|' ${TMPDIR}/stat_activity_table | grep -c -v  'noop *0 wrt *0$' )
if [[ "SOME_WRITE" -gt 0 ]]
then
    FAIL=$((FAIL+1024))
fi    
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 204: failure ($FAIL)"
    echo "info are in $TMPDIR"
    exit 204
fi
echo "Test 204: ok ( $? )"
rm -rf "$TMPDIR"

# test 205 [mysql:mssql] copy small tables sql => count rows / foobar.* => barfoo.* 
TMPDIR=$(mktemp -d )
eval "$BINARY -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -dst-schema barfoo -guessprimarykey -dst-port=8300 -dst-user=admin -dst-pwd=Test+12345 -dst-driver mssql    -dst-db paradump   $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' ) --statsfile ${TMPDIR}/stat_activity_table --writer-no-delete --writer-no-update --writer-no-insert $DEBUG_CMD " || { echo "Test 205: failure" ; exit 205 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    # shellcheck disable=SC2086
    CNT=$(${DCK_MSSQL},8300  -Q "select 'cnt',count(*) as cnt  from foobar.$T "  2>/dev/null | sed 's/^cnt *\([^ ]\)/\1/p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
# shellcheck disable=SC2086
CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8300  -Q "select 'cnt_match',count(*) as cnt_match  from foobar.ticket_tag where convert(varchar(max),CAST(label as varbinary(256)),2) = label_hex_u16le "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+32))
fi
# shellcheck disable=SC2086
CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8300  -Q "select 'cnt_match',count(*) as cnt_match  from foobar.account_metadatas where metasha256 = LOWER(CONVERT(VARCHAR(MAX),HASHBYTES('SHA2_256',metavalue),2)) "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
then
    FAIL=$((FAIL+64))
fi
SOME_WRITE=$( sed 's|^\(sensor_tag.*dst.* noop *\) 17|\1 0|' ${TMPDIR}/stat_activity_table | grep -c -v  'noop *0 wrt *0$' )
if [[ "SOME_WRITE" -gt 0 ]]
then
    FAIL=$((FAIL+1024))
fi    
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 205: failure ($FAIL)"
    echo "info are in $TMPDIR"
    exit 205
fi
echo "Test 205: ok ( $? )"
rm -rf "$TMPDIR"

# test 206 [mysql:mssql] copy small tables sql => count rows / barfoo.* => foobar.*
TMPDIR=$(mktemp -d )
eval "$BINARY -port 4900 -pwd Test+12345 -user foobar  -guessprimarykey -schema barfoo -dst-schema foobar -guessprimarykey -dst-port=8200 -dst-user=admin -dst-pwd=Test+12345 -dst-driver mssql    -dst-db paradump   $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  --statsfile ${TMPDIR}/stat_activity_table --writer-no-delete --writer-no-update --writer-no-insert   $DEBUG_CMD " || { echo "Test 206: failure" ; exit 206 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    # shellcheck disable=SC2086
    CNT=$(${DCK_MSSQL},8200  -Q "select 'cnt',count(*) as cnt  from barfoo.$T "  2>/dev/null | sed 's/^cnt *\([^ ]\)/\1/p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
# shellcheck disable=SC2086
CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8200  -Q "select 'cnt_match',count(*) as cnt_match  from barfoo.ticket_tag where convert(varchar(max),CAST(label as varbinary(256)),2) = label_hex_u16le "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+32))
fi
# shellcheck disable=SC2086
CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8200  -Q "select 'cnt_match',count(*) as cnt_match  from barfoo.account_metadatas where metasha256 = LOWER(CONVERT(VARCHAR(MAX),HASHBYTES('SHA2_256',metavalue),2)) "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
then
    FAIL=$((FAIL+64))
fi
SOME_WRITE=$( sed 's|^\(sensor_tag.*dst.* noop *\) 17|\1 0|' ${TMPDIR}/stat_activity_table | grep -c -v  'noop *0 wrt *0$' )
if [[ "SOME_WRITE" -gt 0 ]]
then
    FAIL=$((FAIL+1024))
fi    
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 206: failure ($FAIL)"
    echo "info are in $TMPDIR"
    exit 206
fi
echo "Test 206: ok ( $? )"
rm -rf "$TMPDIR"
# ------------------------------------------------------------------------------------------

if [ $TESTS_21X -eq 1 ]
then   
    # --------------------------------------------------------------------------------------
    # test 211  copy small tables sql => count rows in foobar
    TMPDIR=$(mktemp -d )
    eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey -dst-port=4900 -dst-user=foobar -dst-pwd=Test+12345      $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  --statsfile ${TMPDIR}/stat_activity_table --writer-no-delete --writer-no-update --writer-no-insert  $DEBUG_CMD " || { echo "Test 211: failure" ; exit 211 ; }
    FAIL=0
    for T in $LIST_SMALL_TABLES
    do
	CNT=$(${DCK_MYSQL} --port 4900 foobar -e "select count(*) as cnt from $T \G" 2>/dev/null | sed 's/^cnt: //p;d')
	if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
	then
	    FAIL=$((FAIL+1))
	fi
	${DCK_MYSQL_DUMP} --port 4900 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact foobar "$T"  2>/dev/null  | grep -a -v '^$' >  "${TMPDIR}/mysqldump_foobar_${T}.sql" 2>/dev/null
    done
    for T in $LIST_SMALL_TABLES
    do
	CNT_LINES_SRC=$(grep -c '^INSERT' "${TMPDIR_T200}/mysqldump_foobar_${T}".sql )
	CNT_LINES_DST=$(grep -c '^INSERT' "${TMPDIR}/mysqldump_foobar_${T}".sql )
	if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_DST" ]]
	then
	    FAIL=$((FAIL+1))
	else
	    DIFF_RES=$(mktemp)
	    diff -u <( sort "${TMPDIR_T200}/mysqldump_foobar_${T}".sql ) <( sort "${TMPDIR}/mysqldump_foobar_${T}".sql )  > "$DIFF_RES"
	    CNT_PLUS=$( grep -c '^+'  "$DIFF_RES" )
	    CNT_MINUS=$( grep -c '^-'  "$DIFF_RES" )
	    if [[ "$CNT_PLUS" -gt 0 || "$CNT_MINUS" -gt 0 ]]
	    then
		FAIL=$((FAIL+1))
	    else
		rm "$DIFF_RES"
	    fi
	fi
    done
    SOME_WRITE=$(grep -c -v  'noop *0 wrt *0$' ${TMPDIR}/stat_activity_table  )
    if [[ "SOME_WRITE" -gt 0 ]]
    then
	FAIL=$((FAIL+1))
    fi    
    if [[ "$FAIL" -gt 0 ]]
    then
	echo "Test 211: failure ($FAIL)"
	echo "info are in $TMPDIR"
	echo "info T200 are in ${TMPDIR_T200}"
	exit 211
    fi
    echo "Test 211: ok ( $? )"
    rm -rf "$TMPDIR"

    # test 212  copy small tables sql => count rows in barfoo
    TMPDIR=$(mktemp -d )
    eval "$BINARY  -dst-port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema barfoo -guessprimarykey -port=4900 -dst-user=foobar -dst-pwd=Test+12345      $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' ) --statsfile ${TMPDIR}/stat_activity_table --writer-no-delete --writer-no-update --writer-no-insert $DEBUG_CMD " || { echo "Test 212: failure" ; exit 212 ; }
    FAIL=0
    for T in $LIST_SMALL_TABLES
    do
	CNT=$(${DCK_MYSQL} --port 4900 barfoo -e "select count(*) as cnt from $T \G" 2>/dev/null | sed 's/^cnt: //p;d')
	if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
	then
	    FAIL=$((FAIL+1))
	fi
	${DCK_MYSQL_DUMP} --port 4900 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact barfoo "$T"  2>/dev/null  | grep -a -v '^$' >  "${TMPDIR}/mysqldump_barfoo_${T}.sql" 2>/dev/null
    done
    for T in $LIST_SMALL_TABLES
    do
	CNT_LINES_SRC=$(grep -c '^INSERT' "${TMPDIR_T200}/mysqldump_foobar_${T}".sql )
	CNT_LINES_DST=$(grep -c '^INSERT' "${TMPDIR}/mysqldump_barfoo_${T}".sql )
	if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_DST" ]]
	then
	    FAIL=$((FAIL+1))
	else
	    DIFF_RES=$(mktemp)
	    diff -u <( sort "${TMPDIR_T200}/mysqldump_foobar_${T}".sql ) <( sort "${TMPDIR}/mysqldump_barfoo_${T}".sql )  > "$DIFF_RES"
	    CNT_PLUS=$( grep -c '^+'  "$DIFF_RES" )
	    CNT_MINUS=$( grep -c '^-'  "$DIFF_RES" )
	    if [[ "$CNT_PLUS" -gt 0 || "$CNT_MINUS" -gt 0 ]]
	    then
		FAIL=$((FAIL+1))
	    else
		rm "$DIFF_RES"
	    fi
	fi
    done
    SOME_WRITE=$(grep -c -v  'noop *0 wrt *0$' ${TMPDIR}/stat_activity_table  )
    if [[ "SOME_WRITE" -gt 0 ]]
    then
	FAIL=$((FAIL+1))
    fi    
    if [[ "$FAIL" -gt 0 ]]
    then
	echo "Test 212: failure ($FAIL)"
	echo "info are in $TMPDIR"
	echo "info T200 are in ${TMPDIR_T200}"
	exit 212
    fi
    echo "Test 212: ok ( $? )"
    rm -rf "$TMPDIR"

    # test 213 cpy small tables into postgres / foobar
    TMPDIR=$(mktemp -d )
    eval "$BINARY -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey -dst-port=8100 -dst-user=admin -dst-pwd=Test+12345 -dst-driver postgres -dst-db paradump  $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' ) --statsfile ${TMPDIR}/stat_activity_table --writer-no-delete --writer-no-update --writer-no-insert $DEBUG_CMD " || { echo "Test 213: failure" ; exit 213 ; }
    FAIL=0
    for T in $LIST_SMALL_TABLES
    do
	CNT=$(${DCK_PSQL} --port 8100 -c "select 'cnt',count(*) as cnt from foobar.$T ;" 2>/dev/null | sed 's/^cnt *: *\([^ ]\)/\1/p;d')
	if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
	then
	    FAIL=$((FAIL+1))
	fi
    done
    CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8100  -c "select 'cnt_match',count(*) as cnt_match  from foobar.ticket_tag where upper(encode(convert_to(label,'UTF8'),'hex')) = label_postgres_hex_u8 ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
    if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
    then
	FAIL=$((FAIL+32))
    fi
    CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8100  -c "select 'cnt_match',count(*) as cnt_match  from foobar.account_metadatas where metasha256 = encode(sha256(metavalue),'hex') ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
    if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
    then
	FAIL=$((FAIL+64))
    fi
    SOME_WRITE=$( sed 's|^\(ticket_tag.*dst.* noop *\) 1|\1 0|' ${TMPDIR}/stat_activity_table | grep -c -v  'noop *0 wrt *0$' )
    if [[ "SOME_WRITE" -gt 0 ]]
    then
	FAIL=$((FAIL+1024))
    fi    
    if [[ "$FAIL" -gt 0 ]]
    then
	echo "Test 213: failure ($FAIL)"
	echo "info are in $TMPDIR"
	exit 213    
    fi
    echo "Test 213: ok ( $? )"
    rm -rf "$TMPDIR"

    # test 214 cpy small tables into postgres / barfoo
    TMPDIR=$(mktemp -d )
    eval "$BINARY -port 4900 -pwd Test+12345 -user foobar  -guessprimarykey -schema barfoo -guessprimarykey -dst-port=8000 -dst-user=admin -dst-pwd=Test+12345 -dst-driver postgres -dst-db paradump  $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  --statsfile ${TMPDIR}/stat_activity_table --writer-no-delete --writer-no-update --writer-no-insert $DEBUG_CMD " || { echo "Test 214: failure" ; exit 214 ; }
    FAIL=0
    for T in $LIST_SMALL_TABLES
    do
	CNT=$(${DCK_PSQL} --port 8000 -c "select 'cnt',count(*) as cnt from barfoo.$T ;" 2>/dev/null | sed 's/^cnt *: *\([^ ]\)/\1/p;d')
	if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
	then
	    FAIL=$((FAIL+1))
	fi
    done
    CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8000  -c "select 'cnt_match',count(*) as cnt_match  from barfoo.ticket_tag where upper(encode(convert_to(label,'UTF8'),'hex')) = label_postgres_hex_u8 ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
    if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
    then
	FAIL=$((FAIL+32))
    fi
    CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8000  -c "select 'cnt_match',count(*) as cnt_match  from barfoo.account_metadatas where metasha256 = encode(sha256(metavalue),'hex') ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
    if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
    then
	FAIL=$((FAIL+64))
    fi
    SOME_WRITE=$( sed 's|^\(ticket_tag.*dst.* noop *\) 1|\1 0|' ${TMPDIR}/stat_activity_table | grep -c -v  'noop *0 wrt *0$' )
    if [[ "SOME_WRITE" -gt 0 ]]
    then
	FAIL=$((FAIL+1024))
    fi    
    if [[ "$FAIL" -gt 0 ]]
    then
	echo "Test 214: failure ($FAIL)"
	echo "info are in $TMPDIR"
	exit 214
    fi
    echo "Test 214: ok ( $? )"
    rm -rf "$TMPDIR"

    # test 215 cpy small tables into mssql / foobar
    TMPDIR=$(mktemp -d )
    eval "$BINARY -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey -dst-port=8300 -dst-user=admin -dst-pwd=Test+12345 -dst-driver mssql    -dst-db paradump   $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' ) --statsfile ${TMPDIR}/stat_activity_table --writer-no-delete --writer-no-update --writer-no-insert $DEBUG_CMD " || { echo "Test 215: failure" ; exit 215 ; }
    FAIL=0
    for T in $LIST_SMALL_TABLES
    do
	# shellcheck disable=SC2086
	CNT=$(${DCK_MSSQL},8300  -Q "select 'cnt',count(*) as cnt  from foobar.$T "  2>/dev/null | sed 's/^cnt *\([^ ]\)/\1/p;d')
	if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
	then
	    FAIL=$((FAIL+1))
	fi
    done
    # shellcheck disable=SC2086
    CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8300  -Q "select 'cnt_match',count(*) as cnt_match  from foobar.ticket_tag where convert(varchar(max),CAST(label as varbinary(256)),2) = label_hex_u16le "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
    if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
    then
	FAIL=$((FAIL+32))
    fi
    # shellcheck disable=SC2086
    CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8300  -Q "select 'cnt_match',count(*) as cnt_match  from foobar.account_metadatas where metasha256 = LOWER(CONVERT(VARCHAR(MAX),HASHBYTES('SHA2_256',metavalue),2)) "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
    if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
    then
	FAIL=$((FAIL+64))
    fi
    SOME_WRITE=$( sed 's|^\(sensor_tag.*dst.* noop *\) 17|\1 0|' ${TMPDIR}/stat_activity_table | grep -c -v  'noop *0 wrt *0$' )
    if [[ "SOME_WRITE" -gt 0 ]]
    then
	FAIL=$((FAIL+1024))
    fi    
    if [[ "$FAIL" -gt 0 ]]
    then
	echo "Test 215: failure ($FAIL)"
	echo "info are in $TMPDIR"
	exit 215
    fi
    echo "Test 215: ok ( $? )"
    rm -rf "$TMPDIR"

    # test 216 cpy small tables into mssql / barfoo
    TMPDIR=$(mktemp -d )
    eval "$BINARY -port 4900 -pwd Test+12345 -user foobar  -guessprimarykey -schema barfoo -guessprimarykey -dst-port=8200 -dst-user=admin -dst-pwd=Test+12345 -dst-driver mssql    -dst-db paradump   $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  --statsfile ${TMPDIR}/stat_activity_table --writer-no-delete --writer-no-update --writer-no-insert   $DEBUG_CMD " || { echo "Test 216: failure" ; exit 216 ; }
    FAIL=0
    for T in $LIST_SMALL_TABLES
    do
	# shellcheck disable=SC2086
	CNT=$(${DCK_MSSQL},8200  -Q "select 'cnt',count(*) as cnt  from barfoo.$T "  2>/dev/null | sed 's/^cnt *\([^ ]\)/\1/p;d')
	if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
	then
	    FAIL=$((FAIL+1))
	fi
    done
    # shellcheck disable=SC2186
    CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8200  -Q "select 'cnt_match',count(*) as cnt_match  from barfoo.ticket_tag where convert(varchar(max),CAST(label as varbinary(256)),2) = label_hex_u16le "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
    if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
    then
	FAIL=$((FAIL+32))
    fi
    # shellcheck disable=SC2086
    CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8200  -Q "select 'cnt_match',count(*) as cnt_match  from barfoo.account_metadatas where metasha256 = LOWER(CONVERT(VARCHAR(MAX),HASHBYTES('SHA2_256',metavalue),2)) "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
    if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
    then
	FAIL=$((FAIL+64))
    fi
    SOME_WRITE=$( sed 's|^\(sensor_tag.*dst.* noop *\) 17|\1 0|' ${TMPDIR}/stat_activity_table | grep -c -v  'noop *0 wrt *0$' )
    if [[ "SOME_WRITE" -gt 0 ]]
    then
	FAIL=$((FAIL+1024))
    fi    
    if [[ "$FAIL" -gt 0 ]]
    then
	echo "Test 216: failure ($FAIL)"
	echo "info are in $TMPDIR"
	exit 216
    fi
    echo "Test 216: ok ( $? )"
    rm -rf "$TMPDIR"
    # --------------------------------------------------------------------------------------
fi
# ------------------------------------------------------------------------------------------
generate_differences() {
    for DB_INF in DCK_MYSQL:4900:foobar DCK_MYSQL:4000:barfoo \
		  DCK_PSQL:8100:foobar DCK_PSQL:8000:barfoo \
		  DCK_MSSQL:8300:foobar DCK_MSSQL:8200:barfoo
    do
	CMD=$( echo $DB_INF | cut -d: -f1 )
	PORT=$( echo $DB_INF | cut -d: -f2 )
	DB=$( echo $DB_INF | cut -d: -f3 )
	if [ $CMD = "DCK_MSSQL" ]
	then
	    EXTRA_CMD=",$PORT"
	    PORT=""
	else
	    EXTRA_CMD=""
	    PORT="--port ${PORT}"
	fi
	(
	cat <<-EOF
	update $DB.ticket_tag set id=cast(substring(replace(replace(replace(replace(replace(replace(replace(label_hex_u8,'0',''),'F',''),'E',''),'D',''),'C',''),'B',''),'A',''),1,10) as decimal )% 4000+id where id % 7 < 2 ;
	update $DB.ticket_tag set label_hex_u8=label , label_hex_l1=label_hex_u8 where id % 7 = 4 ;
	EOF
	) | ${!CMD}${EXTRA_CMD} ${PORT} >/dev/null 2>&1
    done
}
#
generate_differences
#------------------------
#ARG_NOOP="--writer-no-delete --writer-no-update --writer-no-insert"
# ------------------------------------------------------------------------------------------
# test 221  copy small tables sql => count rows in foobar
TMPDIR=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey -dst-port=4900 -dst-user=foobar -dst-pwd=Test+12345      $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  --statsfile ${TMPDIR}/stat_activity_table ${ARG_NOOP}  $DEBUG_CMD " || { echo "Test 221: failure" ; exit 221 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    CNT=$(${DCK_MYSQL} --port 4900 foobar -e "select count(*) as cnt from $T \G" 2>/dev/null | sed 's/^cnt: //p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
    ${DCK_MYSQL_DUMP} --port 4900 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact foobar "$T"  2>/dev/null  | grep -a -v '^$' >  "${TMPDIR}/mysqldump_foobar_${T}.sql" 2>/dev/null
done
for T in $LIST_SMALL_TABLES
do
    CNT_LINES_SRC=$(grep -c '^INSERT' "${TMPDIR_T200}/mysqldump_foobar_${T}".sql )
    CNT_LINES_DST=$(grep -c '^INSERT' "${TMPDIR}/mysqldump_foobar_${T}".sql )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_DST" ]]
    then
	FAIL=$((FAIL+1))
    else
	DIFF_RES=$(mktemp)
	diff -u <( sort "${TMPDIR_T200}/mysqldump_foobar_${T}".sql ) <( sort "${TMPDIR}/mysqldump_foobar_${T}".sql )  > "$DIFF_RES"
	CNT_PLUS=$( grep -c '^+'  "$DIFF_RES" )
	CNT_MINUS=$( grep -c '^-'  "$DIFF_RES" )
	if [[ "$CNT_PLUS" -gt 0 || "$CNT_MINUS" -gt 0 ]]
	then
	    FAIL=$((FAIL+1))
	else
	    rm "$DIFF_RES"
	fi
    fi
done
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 221: failure ($FAIL)"
    echo "info are in $TMPDIR"
    echo "info T200 are in ${TMPDIR_T200}"
    exit 221
fi
echo "Test 221: ok ( $? )"
rm -rf "$TMPDIR"

# test 222  copy small tables sql => count rows in barfoo
TMPDIR=$(mktemp -d )
eval "$BINARY  -dst-port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema barfoo -guessprimarykey -port=4900 -dst-user=foobar -dst-pwd=Test+12345      $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' ) --statsfile ${TMPDIR}/stat_activity_table ${ARG_NOOP} $DEBUG_CMD " || { echo "Test 222: failure" ; exit 222 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    CNT=$(${DCK_MYSQL} --port 4900 barfoo -e "select count(*) as cnt from $T \G" 2>/dev/null | sed 's/^cnt: //p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
    ${DCK_MYSQL_DUMP} --port 4900 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact barfoo "$T"  2>/dev/null  | grep -a -v '^$' >  "${TMPDIR}/mysqldump_barfoo_${T}.sql" 2>/dev/null
done
for T in $LIST_SMALL_TABLES
do
    CNT_LINES_SRC=$(grep -c '^INSERT' "${TMPDIR_T200}/mysqldump_foobar_${T}".sql )
    CNT_LINES_DST=$(grep -c '^INSERT' "${TMPDIR}/mysqldump_barfoo_${T}".sql )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_DST" ]]
    then
	FAIL=$((FAIL+1))
    else
	DIFF_RES=$(mktemp)
	diff -u <( sort "${TMPDIR_T200}/mysqldump_foobar_${T}".sql ) <( sort "${TMPDIR}/mysqldump_barfoo_${T}".sql )  > "$DIFF_RES"
	CNT_PLUS=$( grep -c '^+'  "$DIFF_RES" )
	CNT_MINUS=$( grep -c '^-'  "$DIFF_RES" )
	if [[ "$CNT_PLUS" -gt 0 || "$CNT_MINUS" -gt 0 ]]
	then
	    FAIL=$((FAIL+1))
	else
	    rm "$DIFF_RES"
	fi
    fi
done
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 222: failure ($FAIL)"
    echo "info are in $TMPDIR"
    echo "info T200 are in ${TMPDIR_T200}"
    exit 222
fi
echo "Test 222: ok ( $? )"
rm -rf "$TMPDIR"

# test 223 cpy small tables into postgres / foobar
TMPDIR=$(mktemp -d )
eval "$BINARY -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey -dst-port=8100 -dst-user=admin -dst-pwd=Test+12345 -dst-driver postgres -dst-db paradump  $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' ) --statsfile ${TMPDIR}/stat_activity_table ${ARG_NOOP} $DEBUG_CMD " || { echo "Test 223: failure" ; exit 223 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    CNT=$(${DCK_PSQL} --port 8100 -c "select 'cnt',count(*) as cnt from foobar.$T ;" 2>/dev/null | sed 's/^cnt *: *\([^ ]\)/\1/p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8100  -c "select 'cnt_match',count(*) as cnt_match  from foobar.ticket_tag where upper(encode(convert_to(label,'UTF8'),'hex')) = label_postgres_hex_u8 ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+32))
fi
CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8100  -c "select 'cnt_match',count(*) as cnt_match  from foobar.account_metadatas where metasha256 = encode(sha256(metavalue),'hex') ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
then
    FAIL=$((FAIL+64))
fi
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 223: failure ($FAIL)"
    echo "info are in $TMPDIR"
    exit 223    
fi
echo "Test 223: ok ( $? )"
rm -rf "$TMPDIR"

# test 224 cpy small tables into postgres / barfoo
TMPDIR=$(mktemp -d )
eval "$BINARY -port 4900 -pwd Test+12345 -user foobar  -guessprimarykey -schema barfoo -guessprimarykey -dst-port=8000 -dst-user=admin -dst-pwd=Test+12345 -dst-driver postgres -dst-db paradump  $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  --statsfile ${TMPDIR}/stat_activity_table ${ARG_NOOP} $DEBUG_CMD " || { echo "Test 224: failure" ; exit 224 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    CNT=$(${DCK_PSQL} --port 8000 -c "select 'cnt',count(*) as cnt from barfoo.$T ;" 2>/dev/null | sed 's/^cnt *: *\([^ ]\)/\1/p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8000  -c "select 'cnt_match',count(*) as cnt_match  from barfoo.ticket_tag where upper(encode(convert_to(label,'UTF8'),'hex')) = label_postgres_hex_u8 ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+32))
fi
CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8000  -c "select 'cnt_match',count(*) as cnt_match  from barfoo.account_metadatas where metasha256 = encode(sha256(metavalue),'hex') ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
then
    FAIL=$((FAIL+64))
fi
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 224: failure ($FAIL)"
    echo "info are in $TMPDIR"
    exit 224
fi
echo "Test 224: ok ( $? )"
rm -rf "$TMPDIR"

# test 225 cpy small tables into mssql / foobar
TMPDIR=$(mktemp -d )
eval "$BINARY -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey -dst-port=8300 -dst-user=admin -dst-pwd=Test+12345 -dst-driver mssql    -dst-db paradump   $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' ) --statsfile ${TMPDIR}/stat_activity_table ${ARG_NOOP} $DEBUG_CMD " || { echo "Test 225: failure" ; exit 225 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    # shellcheck disable=SC2086
    CNT=$(${DCK_MSSQL},8300  -Q "select 'cnt',count(*) as cnt  from foobar.$T "  2>/dev/null | sed 's/^cnt *\([^ ]\)/\1/p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
# shellcheck disable=SC2086
CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8300  -Q "select 'cnt_match',count(*) as cnt_match  from foobar.ticket_tag where convert(varchar(max),CAST(label as varbinary(256)),2) = label_hex_u16le "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+32))
fi
# shellcheck disable=SC2086
CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8300  -Q "select 'cnt_match',count(*) as cnt_match  from foobar.account_metadatas where metasha256 = LOWER(CONVERT(VARCHAR(MAX),HASHBYTES('SHA2_256',metavalue),2)) "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
then
    FAIL=$((FAIL+64))
fi
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 225: failure ($FAIL)"
    echo "info are in $TMPDIR"
    exit 225
fi
echo "Test 225: ok ( $? )"
rm -rf "$TMPDIR"

# test 226 cpy small tables into mssql / barfoo
TMPDIR=$(mktemp -d )
eval "$BINARY -port 4900 -pwd Test+12345 -user foobar  -guessprimarykey -schema barfoo -guessprimarykey -dst-port=8200 -dst-user=admin -dst-pwd=Test+12345 -dst-driver mssql    -dst-db paradump   $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  --statsfile ${TMPDIR}/stat_activity_table ${ARG_NOOP}   $DEBUG_CMD " || { echo "Test 226: failure" ; exit 226 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    # shellcheck disable=SC2086
    CNT=$(${DCK_MSSQL},8200  -Q "select 'cnt',count(*) as cnt  from barfoo.$T "  2>/dev/null | sed 's/^cnt *\([^ ]\)/\1/p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
# shellcheck disable=SC2086
CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8200  -Q "select 'cnt_match',count(*) as cnt_match  from barfoo.ticket_tag where convert(varchar(max),CAST(label as varbinary(256)),2) = label_hex_u16le "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+32))
fi
# shellcheck disable=SC2086
CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8200  -Q "select 'cnt_match',count(*) as cnt_match  from barfoo.account_metadatas where metasha256 = LOWER(CONVERT(VARCHAR(MAX),HASHBYTES('SHA2_256',metavalue),2)) "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
then
    FAIL=$((FAIL+64))
fi
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 226: failure ($FAIL)"
    echo "info are in $TMPDIR"
    exit 226
fi
echo "Test 226: ok ( $? )"
rm -rf "$TMPDIR"
