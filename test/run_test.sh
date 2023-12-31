#!/bin/bash

cd "$(dirname "$0")" || exit 200

exec &> >( while read -r L ; do echo "$(date '+%b %d %T')" "$L" ; done )

BINARY=../src/paradump
# ------------------------------------------------------------------------------------------
sleep_1_sec() {
    sleep 1
}
trap sleep_1_sec EXIT
# ------------------------------------------------------------------------------------------
TRUNCATE_TABLE_ONLY=0
SMALL_TESTS_ONLY=0
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
	if [[ "$1" = "--small-tests" ]]
	then
	    SMALL_TESTS_ONLY=1
	    shift
	    continue
	fi
	if [[ "$1" = "--truncate-tables" ]]
	then
	    TRUNCATE_TABLE_ONLY=1
	    shift
	    continue
	fi
	echo "ERROR ARG '$1' is invalid"
	exit 203
done
# ------------------------------------------------------------------------------------------
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
# others tables
#
LIST_SET_3="client_activity location_history ticket_history sensor_info sensors_pairing"

LIST_TABLES="${LIST_SET_1} ${LIST_SET_2} ${LIST_SET_3}"

LIST_SMALL_TABLES="${LIST_SET_1} ${LIST_SET_2}"

LIST_TABLES_CSV="${LIST_SET_2} ${LIST_SET_3}"

# ------------------------------------------------------------------------------------------
truncate_tables() {
    (
	for T in $LIST_TABLES
	do
	    ${DCK_MYSQL} --port 4900 foobar -e "truncate table $T ;" >/dev/null 2>&1 &
	    ${DCK_MYSQL} --port 4000 barfoo -e "truncate table $T ;" >/dev/null 2>&1 &
	    ${DCK_MYSQL} --port 4900 test   -e "truncate table $T ;" >/dev/null 2>&1 &
	    ${DCK_MYSQL} --port 4000 test   -e "truncate table $T ;" >/dev/null 2>&1 &
	    ${DCK_PSQL}  --port 8100        -c "truncate table foobar.$T ;" >/dev/null 2>&1 &
	    ${DCK_PSQL}  --port 8100        -c "truncate table test.$T ;" >/dev/null 2>&1 &
	    ${DCK_PSQL}  --port 8000        -c "truncate table barfoo.$T ;" >/dev/null 2>&1 &
	    ${DCK_PSQL}  --port 8000        -c "truncate table test.$T ;" >/dev/null 2>&1 &
	    # shellcheck disable=SC2086
	    ${DCK_MSSQL},8300               -Q "truncate table foobar.$T  " >/dev/null 2>&1 &
	    # shellcheck disable=SC2086
	    ${DCK_MSSQL},8300               -Q "truncate table test.$T  " >/dev/null 2>&1 &
	    # shellcheck disable=SC2086
	    ${DCK_MSSQL},8200               -Q "truncate table barfoo.$T  " >/dev/null 2>&1 &
	    # shellcheck disable=SC2086
	    ${DCK_MSSQL},8200               -Q "truncate table test.$T  " >/dev/null 2>&1 &
	    wait
	done
    )
}
# ------------------------------------------------------------------------------------------
echo "Init   0:"

truncate_tables
if [ "$TRUNCATE_TABLE_ONLY" -eq 1 ]
then
    exit 0
fi    
echo "Check  0:"
# order in loops matter's because 4000/foobar is the reference 
for port in 4900 4000
do
    echo
    for T in $LIST_TABLES
    do
	for DB in barfoo foobar
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

# test  1 , need args
eval "$BINARY                                                                                                          $DEBUG_CMD " && echo "Test   1: failure" && exit  1
echo "Test   1: ok ( $? )"

# test  2 , need args
eval "$BINARY  -schema foobar             		   	     		       			             	       $DEBUG_CMD " && echo "Test   2: failure" && exit  2
echo "Test   2: ok ( $? )"

# test  3 , need args
eval "$BINARY  -schema foobar -alltables  		   	     		       			             	       $DEBUG_CMD " && echo "Test   3: failure" && exit  3
echo "Test   3: ok ( $? )"

# test  4 , need args
eval "$BINARY  -schema foobar -alltables --port 4000 		  	       			             	       $DEBUG_CMD " && echo "Test   4: failure" && exit  4
echo "Test   4: ok ( $? )"

# test  5 , need args
eval "$BINARY  -schema foobar -alltables  -table client_info 	     		       			     	       $DEBUG_CMD " && echo "Test   5: failure" && exit  5
echo "Test   5: ok ( $? )"

# test  6 , need args
eval "$BINARY  -table client_info            		   	     		       			     	       $DEBUG_CMD " && echo "Test   6: failure" && exit  6
echo "Test   6: ok ( $? )"

# test  7 , need args
eval "$BINARY  -schema foobar -schema test -table client_info    	     		       			     	       $DEBUG_CMD " && echo "Test   7: failure" && exit  7
echo "Test   7: ok ( $? )"

# test  8 , need args
eval "$BINARY  -schema foobar -schema test                       	     		       			     	       $DEBUG_CMD " && echo "Test   8: failure" && exit  8
echo "Test   8: ok ( $? )"

# test  9 , need args
eval "$BINARY  -schema foobar -schema test -alltables -dumpfile ''         		       			     	       $DEBUG_CMD " && echo "Test   9: failure" && exit  9
echo "Test   9: ok ( $? )"

# test 10 , need args
eval "$BINARY  -schema foobar -schema test -alltables -dumpmode 'ods'      		       			     	       $DEBUG_CMD " && echo "Test  10: failure" && exit 10
echo "Test  10: ok ( $? )"

# test 11 , need args
eval "$BINARY  -schema foobar -schema test -alltables -dumpcompress 'gzip' 		       			     	       $DEBUG_CMD " && echo "Test  11: failure" && exit 11
echo "Test  11: ok ( $? )"

# test 12 , need args
eval "$BINARY  -schema foobar -schema test -alltables -chunksize 40        		       			     	       $DEBUG_CMD " && echo "Test  12: failure" && exit 12
echo "Test  12: ok ( $? )"

# test 13 , need args
eval "$BINARY  -schema foobar -schema test -alltables -port 4000 -pwd Test+12345 -user foobar  			     	       $DEBUG_CMD " && echo "Test  13: failure" && exit 13
echo "Test  13: ok ( $? )"

# test 14 , bad database
eval "$BINARY  -schema foobar -schema foobar_copy -alltables -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey 	       $DEBUG_CMD " && echo "Test  14: failure" && exit 14
echo "Test  14: ok ( $? )"

# test 15 , bad table
eval "$BINARY  -schema foobar -port 4000 -pwd Test+12345 -user foobar -table a_very_bad_table                      	       $DEBUG_CMD " && echo "Test  15: failure" && exit 15
echo "Test  15: ok ( $? )"

# test 16 , not a regular table
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema information_schema -table views   	       $DEBUG_CMD " && echo "Test  16: failure" && exit 16
echo "Test  16: ok ( $? )"

# test 17 , not a innodb table
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema mysql -table users                	       $DEBUG_CMD " && echo "Test  17: failure" && exit 17
echo "Test  17: ok ( $? )"

# test 18 , not a regular table
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -table client_report       	       $DEBUG_CMD " && echo "Test  18: failure" && exit 18
echo "Test  18: ok ( $? )"

# test 19 , compression not avaliable for cpy
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -table client_info -schema foobar --dumpmode cpy  -dumpcompress zstd $DEBUG_CMD " && echo "Test  19: failure" && exit 19
echo "Test  19: ok ( $? )"

# test 20 , bad argument
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -table client_info -schema foobar barfoo -guessprimarykey            $DEBUG_CMD " && echo "Test  20: failure" && exit 20
echo "Test  20: ok ( $? )"

# test 21 , dumpfile & dumpdir
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -table client_info -schema foobar -guessprimarykey  --dumpdir=/tmp/ --dumpfile=/tmp/hjk            $DEBUG_CMD " && echo "Test  21: failure" && exit 21
echo "Test  21: ok ( $? )"

# test 22 , dumpfile bad template var
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -table client_info -schema foobar -guessprimarykey  --dumpdir=/tmp/ --dumpfile=dump_%r             $DEBUG_CMD " && echo "Test  22: failure" && exit 22
echo "Test  22: ok ( $? )"

# test 23 , dumpfile % at the end that is not duplicated
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -table client_info -schema foobar -guessprimarykey  --dumpdir=/tmp/ --dumpfile=dump_%z%            $DEBUG_CMD " && echo "Test  23: failure" && exit 23
echo "Test  23: ok ( $? )"

# test 24 , schema is specified twice
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -schema barfoo -schema barfoo -guessprimarykey  -alltables                                             $DEBUG_CMD " && echo "Test  24: failure" && exit 24
echo "Test  24: ok ( $? )"

# test 25 , table is specified twice
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -table client_info -table sensor_tag -table client_info -schema barfoo -guessprimarykey            $DEBUG_CMD " && echo "Test  25: failure" && exit 25
echo "Test  25: ok ( $? )"

# test 26 , db is specified for default ( mysql )
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -db foobar -table client_info -table sensor_tag -schema barfoo -guessprimarykey            $DEBUG_CMD " && echo "Test  26: failure" && exit 26
echo "Test  26: ok ( $? )"

# test 27 , db is specified for mysql
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -driver mysql -db foobar -table client_info -table sensor_tag -schema barfoo -guessprimarykey            $DEBUG_CMD " && echo "Test  27: failure" && exit 27
echo "Test  27: ok ( $? )"

# test 28 , db is not specified for src postgres
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -driver postgres -table client_info -table sensor_tag -schema barfoo -guessprimarykey            $DEBUG_CMD " && echo "Test  28: failure" && exit 28
echo "Test  28: ok ( $? )"

# test 29 , dmode cpy dst-db is specified for default ( mysql ) 
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -dst-db foobar -table client_info -table sensor_tag -schema barfoo -guessprimarykey --dumpmode cpy           $DEBUG_CMD " && echo "Test  29: failure" && exit 29
echo "Test  29: ok ( $? )"

# test 30 , dst-db is specified for mysql
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -dst-driver mysql -dst-db foobar -table client_info -table sensor_tag -schema barfoo -guessprimarykey  --dumpmode cpy $DEBUG_CMD " && echo "Test  30: failure" && exit 30
echo "Test  30: ok ( $? )"

# test 31 , db is not specified for dst postgres
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -dst-driver postgres -table client_info -table sensor_tag -schema barfoo -guessprimarykey --dumpmode cpy              $DEBUG_CMD " && echo "Test  31: failure" && exit 31
echo "Test  31: ok ( $? )"

# test 32 , db is not specified for dst mssql
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -dst-driver mssql -table client_info -table sensor_tag -schema barfoo -guessprimarykey --dumpmode cpy              $DEBUG_CMD " && echo "Test  32: failure" && exit 32
echo "Test  32: ok ( $? )"

# test 33 , dmode cpy duplicate schema 
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar --alltables -schema barfoo -schema barfoo -guessprimarykey            $DEBUG_CMD " && echo "Test  33: failure" && exit 33
echo "Test  33: ok ( $? )"

# test 34 , dmode cpy duplicate dst-schema
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar --alltables -schema barfoo -schema foobar -guessprimarykey --dumpmode cpy  -dst-schema foobar -dst-schema foobar         $DEBUG_CMD " && echo "Test  34: failure" && exit 34
echo "Test  34: ok ( $? )"

# test 100  dump client_info ticket_tag sql insertsize 1 => count lines and compare with mysqldump
TMPDIR_T100=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey --dumpmode sql -dumpfile '${TMPDIR_T100}/dump_%d_%t_%p%m%z' --dumpinsert simple  --dumpheader=false -insertsize 1 $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' ) $DEBUG_CMD " || {  echo "Test 100: failure" ; exit 100 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    SQL_CNT=0
    for F in "${TMPDIR_T100}/dump_foobar_${T}"_*.sql
    do
	if [[ -s "$F" ]]
	then
	    SQL_CNT=$(( SQL_CNT + $( grep -c '^INSERT' < "$F" ) ))
	fi
    done
    if [[ "$SQL_CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
    ${DCK_MYSQL_DUMP} --port 4000 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact foobar "$T"  2>/dev/null  | grep -a -v '^$' >  "${TMPDIR_T100}/mysqldump_foobar_${T}.sql" 2>/dev/null
done
for T in $LIST_SMALL_TABLES
do
    CNT_LINES_SRC=$(cat "${TMPDIR_T100}/dump_foobar_${T}"_*.sql | LANG=C grep -c '^INSERT' )
    CNT_LINES_MDP=$(LANG=C grep -c '^INSERT' < "${TMPDIR_T100}/mysqldump_foobar_${T}".sql )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_MDP" ]]
    then
	FAIL=$((FAIL+1))
    else
	DIFF_RES=$(mktemp "$TMPDIR_T100/diff_XXXXXXX")
	LANG=C diff -a -u <(LANG=C  sort "${TMPDIR_T100}/dump_foobar_${T}"_*.sql ) <(LANG=C sort "${TMPDIR_T100}/mysqldump_foobar_${T}".sql )  > "$DIFF_RES"
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
    echo "Test 100: failure ($FAIL)"
    echo "info are in $TMPDIR_T100"
    exit 100
fi
echo "Test 100: ok ( $? )"

# test 101  copy small tables sql => count rows in foobar
TMPDIR=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey --dumpmode cpy -dst-port=4900 -dst-user=foobar -dst-pwd=Test+12345      $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  $DEBUG_CMD " || { echo "Test 101: failure" ; exit 101 ; }
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
    CNT_LINES_SRC=$(grep -c '^INSERT' "${TMPDIR_T100}/mysqldump_foobar_${T}".sql )
    CNT_LINES_DST=$(grep -c '^INSERT' "${TMPDIR}/mysqldump_foobar_${T}".sql )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_DST" ]]
    then
	FAIL=$((FAIL+1))
    else
	DIFF_RES=$(mktemp)
	diff -u <( sort "${TMPDIR_T100}/mysqldump_foobar_${T}".sql ) <( sort "${TMPDIR}/mysqldump_foobar_${T}".sql )  > "$DIFF_RES"
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
    echo "Test 101: failure ($FAIL)"
    echo "info are in $TMPDIR"
    echo "info T100 are in ${TMPDIR_T100}"
    exit 101
fi
echo "Test 101: ok ( $? )"
rm -rf "$TMPDIR"

# test 102  copy small tables sql => count rows in barfoo
TMPDIR=$(mktemp -d )
eval "$BINARY  -dst-port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema barfoo -guessprimarykey --dumpmode cpy -port=4900 -dst-user=foobar -dst-pwd=Test+12345      $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  $DEBUG_CMD " || { echo "Test 102: failure" ; exit 102 ; }
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
    CNT_LINES_SRC=$(grep -c '^INSERT' "${TMPDIR_T100}/mysqldump_foobar_${T}".sql )
    CNT_LINES_DST=$(grep -c '^INSERT' "${TMPDIR}/mysqldump_barfoo_${T}".sql )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_DST" ]]
    then
	FAIL=$((FAIL+1))
    else
	DIFF_RES=$(mktemp)
	diff -u <( sort "${TMPDIR_T100}/mysqldump_foobar_${T}".sql ) <( sort "${TMPDIR}/mysqldump_barfoo_${T}".sql )  > "$DIFF_RES"
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
    echo "Test 102: failure ($FAIL)"
    echo "info are in $TMPDIR"
    echo "info T100 are in ${TMPDIR_T100}"
    exit 102
fi
echo "Test 102: ok ( $? )"
rm -rf "$TMPDIR"

# test 103 cpy small tables into postgres / foobar
eval "$BINARY -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey --dumpmode cpy -dst-port=8100 -dst-user=admin -dst-pwd=Test+12345 -dst-driver postgres -dst-db paradump  $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  $DEBUG_CMD " || { echo "Test 103: failure" ; exit 103 ; }
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
    echo "Test 103: failure ($FAIL)" && exit 103
fi
echo "Test 103: ok ( $? )"

# test 104 cpy small tables into postgres / barfoo
eval "$BINARY -port 4900 -pwd Test+12345 -user foobar  -guessprimarykey -schema barfoo -guessprimarykey --dumpmode cpy -dst-port=8000 -dst-user=admin -dst-pwd=Test+12345 -dst-driver postgres -dst-db paradump  $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  $DEBUG_CMD " || { echo "Test 104: failure" ; exit 104 ; }
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
    echo "Test 104: failure ($FAIL)" && exit 104
fi
echo "Test 104: ok ( $? )"

# test 105 cpy small tables into mssql / foobar
eval "$BINARY -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey --dumpmode cpy -dst-port=8300 -dst-user=admin -dst-pwd=Test+12345 -dst-driver mssql    -dst-db paradump   $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )     $DEBUG_CMD " || { echo "Test 105: failure" ; exit 105 ; }
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
    echo "Test 105: failure ($FAIL)" && exit 105
fi
echo "Test 105: ok ( $? )"

# test 106 cpy small tables into mssql / barfoo
eval "$BINARY -port 4900 -pwd Test+12345 -user foobar  -guessprimarykey -schema barfoo -guessprimarykey --dumpmode cpy -dst-port=8200 -dst-user=admin -dst-pwd=Test+12345 -dst-driver mssql    -dst-db paradump   $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )     $DEBUG_CMD " || { echo "Test 106: failure" ; exit 106 ; }
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
    echo "Test 106: failure ($FAIL)" && exit 106
fi
echo "Test 106: ok ( $? )"

# test 107  copy small tables foobar  into mysql / test
TMPDIR=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey --dumpmode cpy -dst-port=4900 -dst-schema test -dst-user=apptest -dst-pwd=Test-12345+abc      $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  $DEBUG_CMD " || { echo "Test 107: failure" ; exit 107 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    CNT=$(${DCK_MYSQL} --port 4900 test -e "select count(*) as cnt from $T \G" 2>/dev/null | sed 's/^cnt: //p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
    ${DCK_MYSQL_DUMP} --port 4900 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact test "$T"  2>/dev/null  | grep -a -v '^$' >  "${TMPDIR}/mysqldump_test_${T}.sql" 2>/dev/null
done
for T in $LIST_SMALL_TABLES
do
    CNT_LINES_SRC=$(grep -c '^INSERT' "${TMPDIR_T100}/mysqldump_foobar_${T}".sql )
    CNT_LINES_DST=$(grep -c '^INSERT' "${TMPDIR}/mysqldump_test_${T}".sql )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_DST" ]]
    then
	FAIL=$((FAIL+1))
    else
	DIFF_RES=$(mktemp)
	diff -u <( sort "${TMPDIR_T100}/mysqldump_foobar_${T}".sql ) <( sort "${TMPDIR}/mysqldump_test_${T}".sql )  > "$DIFF_RES"
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
    echo "Test 107: failure ($FAIL)"
    echo "info are in $TMPDIR"
    echo "info T100 are in ${TMPDIR_T100}"
    exit 107
fi
echo "Test 107: ok ( $? )"
rm -rf "$TMPDIR"

# test 108 cpy small tables from foobar into postgres / test 
eval "$BINARY -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey --dumpmode cpy -dst-port=8100 -dst-schema test -dst-user=apptest -dst-pwd=Test-12345+abc -dst-driver postgres -dst-db paradump  $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )  $DEBUG_CMD " || { echo "Test 108: failure" ; exit 108 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    CNT=$(${DCK_PSQL} --port 8100 -c "select 'cnt',count(*) as cnt from test.$T ;" 2>/dev/null | sed 's/^cnt *: *\([^ ]\)/\1/p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8100  -c "select 'cnt_match',count(*) as cnt_match  from test.ticket_tag where upper(encode(convert_to(label,'UTF8'),'hex')) = label_postgres_hex_u8 ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+32))
fi
CNT_TAG_MATCH_U8=$(${DCK_PSQL} --port 8100  -c "select 'cnt_match',count(*) as cnt_match  from test.account_metadatas where metasha256 = encode(sha256(metavalue),'hex') ;"  2>/dev/null | sed 's/^cnt_match *: *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
then
    FAIL=$((FAIL+64))
fi
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 108: failure ($FAIL)" && exit 108
fi
echo "Test 108: ok ( $? )"

# test 109 cpy small tables from foobar into mssql / test
eval "$BINARY -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -guessprimarykey --dumpmode cpy -dst-port=8300 -dst-schema test -dst-user=apptest -dst-pwd=Test-12345+abc -dst-driver mssql    -dst-db paradump   $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' )     $DEBUG_CMD " || { echo "Test 109: failure" ; exit 109 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    # shellcheck disable=SC2086
    CNT=$(${DCK_MSSQL},8300  -Q "select 'cnt',count(*) as cnt  from test.$T "  2>/dev/null | sed 's/^cnt *\([^ ]\)/\1/p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
# shellcheck disable=SC2086
CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8300  -Q "select 'cnt_match',count(*) as cnt_match  from test.ticket_tag where convert(varchar(max),CAST(label as varbinary(256)),2) = label_hex_u16le "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+32))
fi
# shellcheck disable=SC2086
CNT_TAG_MATCH_U8=$(${DCK_MSSQL},8300  -Q "select 'cnt_match',count(*) as cnt_match  from test.account_metadatas where metasha256 = LOWER(CONVERT(VARCHAR(MAX),HASHBYTES('SHA2_256',metavalue),2)) "  2>/dev/null | sed 's/^cnt_match *\([^ ]\)/\1/p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_account_metadatas" )" ]]
then
    FAIL=$((FAIL+64))
fi
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 109: failure ($FAIL)" && exit 109
fi
echo "Test 109: ok ( $? )"

if [ $SMALL_TESTS_ONLY -eq 1 ]
then
    exit 0
fi
truncate_tables

# test 115  dump whole database csv with no header => count lines
TMPDIR_T115=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -alltables -guessprimarykey --dumpmode csv --dumpheader=false -dumpfile '${TMPDIR_T115}/dump_%d_%t_%p%m%z' $DEBUG_CMD " || { echo "Test 115: failure" ; exit 115 ; }
FAIL=0
for T in $LIST_TABLES_CSV
do
    CSV_CNT=0
    for F in "${TMPDIR_T115}/dump_foobar_${T}"_*.csv
    do
	if [[ -s "$F" ]]
	then
	    CSV_CNT=$(( CSV_CNT + $( wc -l < "$F" ) ))
	fi
    done
    if [[ "$CSV_CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 115: failure ($FAIL)"
    echo "info are in $TMPDIR_T115"
    exit 115
fi
echo "Test 115: ok ( $? )"

# test 116  dump whole database csv => count lines
TMPDIR=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -alltables -guessprimarykey --dumpmode csv -dumpfile '${TMPDIR}/dump_%d_%t_%p%m%z' $DEBUG_CMD " || { echo "Test 116: failure" ; exit 116 ; }
FAIL=0
for T in $LIST_TABLES_CSV
do
    CSV_CNT=0
    for F in "${TMPDIR}/dump_foobar_${T}"_*.csv
    do
	if [[ -s "$F" ]]
	then
	    CSV_CNT=$(( CSV_CNT - 1 + $( wc -l < "$F" ) ))
	fi
    done
    if [[ "$CSV_CNT" -ne  "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 116: failure ($FAIL)" && exit 116
fi
echo "Test 116: ok ( $? )"
rm -rf "$TMPDIR"

# test 117  dump whole database csv / zstd => count lines
TMPDIR=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -alltables -guessprimarykey --dumpmode csv -dumpfile '${TMPDIR}/dump_%d_%t_%p%m%z' -dumpcompress zstd $DEBUG_CMD " || { echo "Test 117: failure" ; exit 117 ; }
FAIL=0
for T in $LIST_TABLES_CSV
do
    CSV_CNT=0
    for F in "${TMPDIR}/dump_foobar_${T}"_*.csv.zst
    do
	if [[ -s "$F" ]]
	then
	    CSV_CNT=$(( CSV_CNT - 1 + $( zstdcat "$F" | wc -l ) ))
	fi
    done
    if [[ "$CSV_CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 117: failure ($FAIL)" && exit 117
fi
echo "Test 117: ok ( $? )"
rm -rf "$TMPDIR"

# test 121  dump whole database sql => count lines
TMPDIR_T121=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -alltables -guessprimarykey --dumpmode sql -dumpfile '${TMPDIR_T121}/dump_%d_%t_%p%m%z' --dumpinsert simple  --dumpheader=false -insertsize 1 $DEBUG_CMD " || {  echo "Test 121: failure" ; exit 121 ; }
FAIL=0
for T in $LIST_TABLES
do
    SQL_CNT=0
    for F in "${TMPDIR_T121}/dump_foobar_${T}"_*.sql
    do
	if [[ -s "$F" ]]
	then
	    SQL_CNT=$(( SQL_CNT + $( grep -c '^INSERT' < "$F" ) ))
	fi
    done
    if [[ "$SQL_CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
    ${DCK_MYSQL_DUMP} --port 4000 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact foobar "$T"  2>/dev/null  | grep -a -v '^$' >  "${TMPDIR_T121}/mysqldump_foobar_${T}.sql" 2>/dev/null
done
for T in $LIST_TABLES
do
    CNT_LINES_SRC=$(cat "${TMPDIR_T121}/dump_foobar_${T}"_*.sql | LANG=C grep -c '^INSERT' )
    CNT_LINES_MDP=$(LANG=C wc -l < "${TMPDIR_T121}/mysqldump_foobar_${T}".sql )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_MDP" ]]
    then
	FAIL=$((FAIL+1))
    else
	DIFF_RES=$(mktemp "${TMPDIR_T121}/diff_XXXXXXX")
	LANG=C diff -a -u <(LANG=C sort "${TMPDIR_T121}/dump_foobar_${T}"_*.sql ) <(LANG=C sort "${TMPDIR_T121}/mysqldump_foobar_${T}".sql )  > "$DIFF_RES"
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
    echo "Test 121: failure ($FAIL)"
    echo "info are in $TMPDIR_T121"
    exit 121
fi
echo "Test 121: ok ( $? )"

# test 122  dump whole database sql / zstd => count lines
TMPDIR=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -alltables -guessprimarykey --dumpmode sql -dumpfile '${TMPDIR}/dump_%d_%t_%p%m%z' -dumpcompress zstd -insertsize 1 $DEBUG_CMD " || { echo "Test 122: failure" ; exit 122 ; }
FAIL=0
for T in $LIST_TABLES
do
    SQL_CNT=0
    for F in "${TMPDIR}/dump_foobar_${T}"_*.sql.zst
    do
	if [[ -s "$F" ]]
	then
	    SQL_CNT=$(( SQL_CNT + $( zstdcat "$F" | grep -c '^INSERT' ) ))
	fi
    done
    if [[ "$SQL_CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 122: failure ($FAIL)" && exit 122
fi
echo "Test 122: ok ( $? )"
rm -rf "$TMPDIR"

# test 130  copy whole database sql => count rows in foobar
eval "$BINARY  -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -alltables -guessprimarykey --dumpmode cpy -dst-port=4900 -dst-user=foobar -dst-pwd=Test+12345                     $DEBUG_CMD " || { echo "Test 130: failure" ; exit 130 ; }
FAIL=0
for T in $LIST_TABLES
do
    CNT=$(${DCK_MYSQL} --port 4900 foobar -e "select count(*) as cnt from $T \G" 2>/dev/null | sed 's/^cnt: //p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
CNT_TAG_MATCH_U8=$(${DCK_MYSQL} --port 4900 foobar -e "select count(*) as cnt_match  from ticket_tag where label_hex_u8 = hex(cast(convert(label using utf8mb4)  as binary)) \G" 2>/dev/null | sed 's/^cnt_match: //p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+32))
fi
CNT_TAG_MATCH_L1=$(${DCK_MYSQL}  --port 4900 foobar -e "select count(*) as cnt_match  from ticket_tag where label_hex_l1 = hex(cast(convert(label using latin1)  as binary)) \G" 2>/dev/null | sed 's/^cnt_match: //p;d'  )
if [[ "$CNT_TAG_MATCH_L1" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+64))
fi
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 130: failure ($FAIL)" && exit 130
fi
echo "Test 130: ok ( $? )"

# test 131  dump whole database csv => count lines
eval "$BINARY  -port 4900 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -alltables -guessprimarykey -dumpmode csv -dumpheader=false -dumpfile '${TMPDIR_T115}/dump_%d_copy_%t_%p%m%z' $DEBUG_CMD " || { echo "Test 131: failure" ; exit 131  ; }
FAIL=0
for T in $LIST_TABLES_CSV
do
    DIFF_RES=$(mktemp)
    CNT_LINES_SRC=$(cat "${TMPDIR_T115}/dump_foobar_${T}"_*.csv | wc -l )
    CNT_LINES_DST=$(cat "${TMPDIR_T115}/dump_foobar_copy_${T}"_*.csv | wc -l )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_DST" ]]
    then
	FAIL=$((FAIL+1))
    else
	diff -u <( sort "${TMPDIR_T115}/dump_foobar_${T}"_*.csv ) <( sort "${TMPDIR_T115}/dump_foobar_copy_${T}"_*.csv )  > "$DIFF_RES"
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
    echo "Test 131: failure ($FAIL)" && exit 131
fi
echo "Test 131: ok ( $? )"
# test 132  dump whole database sql => count lines and compare mysqldump from t121 and from copy db inserted by T130
eval "$BINARY  -port 4900 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -alltables -guessprimarykey --dumpmode sql -dumpfile '${TMPDIR_T121}/dump_%d_copy_%t_%p%m%z' --dumpinsert simple  --dumpheader=false -insertsize 1 $DEBUG_CMD " || {  echo "Test 132: failure" ; exit 132 ; }
FAIL=0
for T in $LIST_TABLES
do
    SQL_CNT=0
    for F in "${TMPDIR_T121}/dump_"*"_copy_${T}_"*.sql
    do
	if [[ -s "$F" ]]
	then
	    SQL_CNT=$(( SQL_CNT + $( grep -c '^INSERT' < "$F" ) ))
	fi
    done
    if [[ "$SQL_CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
    ${DCK_MYSQL_DUMP} --port 4900 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact foobar "$T"  2>/dev/null  | grep -a -v '^$' >  "${TMPDIR_T121}/mysqldump_foobar_copy_${T}.sql" 2>/dev/null
done
for T in $LIST_TABLES
do
    CNT_LINES_SRC=$(cat "${TMPDIR_T121}/dump_foobar_${T}"_*.sql | grep -c '^INSERT' )
    CNT_LINES_DST=$(cat "${TMPDIR_T121}/dump_foobar_copy_${T}"_*.sql | grep -c '^INSERT' )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_DST" ]]
    then
	FAIL=$((FAIL+1))
    else
	DIFF_RES=$(mktemp)
	diff -u <( sort "${TMPDIR_T121}/dump_foobar_${T}"_*.sql ) <( sort "${TMPDIR_T121}/dump_foobar_copy_${T}"_*.sql )  > "$DIFF_RES"
	CNT_PLUS=$( grep -c '^+'  "$DIFF_RES" )
	CNT_MINUS=$( grep -c '^-'  "$DIFF_RES" )
	if [[ "$CNT_PLUS" -gt 0 || "$CNT_MINUS" -gt 0 ]]
	then
	    FAIL=$((FAIL+1))
	else
	    rm "$DIFF_RES"
	fi
	DIFF_RES=$(mktemp)
	diff -u <( sort "${TMPDIR_T121}/mysqldump_foobar_${T}".sql ) <( sort "${TMPDIR_T121}/mysqldump_foobar_copy_${T}".sql )  > "$DIFF_RES"
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
    echo "Test 132: failure ($FAIL)" && exit 132
fi
echo "Test 132: ok ( $? )"


# test 140  copy whole database sql into postgress => count rows in foobar / check ticket_tag.label , account_metadatas.metavalue
eval "$BINARY -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -alltables -guessprimarykey --dumpmode cpy -dst-port=8100 -dst-user=admin -dst-pwd=Test+12345 -dst-driver postgres -dst-db paradump        $DEBUG_CMD " || { echo "Test 140: failure" ; exit 140 ; }
FAIL=0
for T in $LIST_TABLES
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
    echo "Test 140: failure ($FAIL)" && exit 140
fi
echo "Test 140: ok ( $? )"

# test 150  copy whole database sql into mssql => count rows in foobar / check ticket_tag.label , account_metadatas.metavalue
eval "$BINARY -port 4000 -pwd Test+12345 -user foobar  -guessprimarykey -schema foobar -alltables -guessprimarykey --dumpmode cpy -dst-port=8300 -dst-user=admin -dst-pwd=Test+12345 -dst-driver mssql    -dst-db paradump        $DEBUG_CMD " || { echo "Test 150: failure" ; exit 150 ; }
FAIL=0
for T in $LIST_TABLES
do
    CNT=$(${DCK_PSQL} --port 8100 -c "select 'cnt',count(*) as cnt from foobar.$T ;" 2>/dev/null | sed 's/^cnt *: *\([^ ]\)/\1/p;d')
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
    echo "Test 150: failure ($FAIL)" && exit 150
fi
echo "Test 150: ok ( $? )"


rm -rf "$TMPDIR_T100"
rm -rf "$TMPDIR_T115"
rm -rf "$TMPDIR_T121"
