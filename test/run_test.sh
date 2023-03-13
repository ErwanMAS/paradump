#!/bin/bash

cd "$(dirname "$0")" || exit 200

exec &> >( while read -r L ; do echo "$(date '+%b %d %T')" "$L" ; done )

BINARY=../src/paradump

DEBUG_CMD=">/dev/null 2>&1"

if [[ "$1" = "--debug" ]]
then
    set -x
    DEBUG_CMD=""
fi

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

DCK_MYSQL="$NEED_SUDO docker run --network=host -i bitnami/mysql:5.7.41  /opt/bitnami/mysql/bin/mysql"
DCK_MYSQL_DUMP="$NEED_SUDO docker run --network=host -i bitnami/mysql:5.7.41  /opt/bitnami/mysql/bin/mysqldump"


# tables that have binary or line return that will prevent to use CSV
LIST_SET_1='ticket_tag account_metadatas'
# tables that are small
LIST_SET_2="client_info text_notifications mail_queue sensor_tag"
# others tables
LIST_SET_3="client_activity location_history ticket_history sensor_info sensors_pairing"

LIST_TABLES="${LIST_SET_1} ${LIST_SET_2} ${LIST_SET_3}"

LIST_SMALL_TABLES="${LIST_SET_1} ${LIST_SET_2}"

LIST_TABLES_CSV="${LIST_SET_2} ${LIST_SET_3}"

echo "Init   0:"
for T in $LIST_TABLES
do
    ${DCK_MYSQL}  -u foobar -ptest1234 --port 5000 -h 127.0.0.1 foobar -e "truncate table $T ;" >/dev/null 2>&1
    ${DCK_MYSQL}  -u foobar -ptest1234 --port 4000 -h 127.0.0.1 barfoo -e "truncate table $T ;" >/dev/null 2>&1
done

echo "Check  0:"
for port in 5000 4000
do
    echo
    for T in $LIST_TABLES
    do
	for DB in barfoo foobar
	do
	    CNT=$(${DCK_MYSQL}  -u foobar -ptest1234 --port $port  -h 127.0.0.1 $DB -e "select count(*) as cnt from $T \G" 2>/dev/null | sed 's/^cnt: //p;d')
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
eval "$BINARY  -db foobar             		   	     		       			             	       $DEBUG_CMD " && echo "Test   2: failure" && exit  2
echo "Test   2: ok ( $? )"

# test  3 , need args
eval "$BINARY  -db foobar -alltables  		   	     		       			             	       $DEBUG_CMD " && echo "Test   3: failure" && exit  3
echo "Test   3: ok ( $? )"

# test  4 , need args
eval "$BINARY  -db foobar -alltables --port 4000 		  	       			             	       $DEBUG_CMD " && echo "Test   4: failure" && exit  4
echo "Test   4: ok ( $? )"

# test  5 , need args
eval "$BINARY  -db foobar -alltables  -table client_info 	     		       			     	       $DEBUG_CMD " && echo "Test   5: failure" && exit  5
echo "Test   5: ok ( $? )"

# test  6 , need args
eval "$BINARY  -table client_info            		   	     		       			     	       $DEBUG_CMD " && echo "Test   6: failure" && exit  6
echo "Test   6: ok ( $? )"

# test  7 , need args
eval "$BINARY  -db foobar -db test -table client_info    	     		       			     	       $DEBUG_CMD " && echo "Test   7: failure" && exit  7
echo "Test   7: ok ( $? )"

# test  8 , need args
eval "$BINARY  -db foobar -db test                       	     		       			     	       $DEBUG_CMD " && echo "Test   8: failure" && exit  8
echo "Test   8: ok ( $? )"

# test  9 , need args
eval "$BINARY  -db foobar -db test -alltables -dumpfile ''         		       			     	       $DEBUG_CMD " && echo "Test   9: failure" && exit  9
echo "Test   9: ok ( $? )"

# test 10 , need args
eval "$BINARY  -db foobar -db test -alltables -dumpmode 'ods'      		       			     	       $DEBUG_CMD " && echo "Test  10: failure" && exit 10
echo "Test  10: ok ( $? )"

# test 11 , need args
eval "$BINARY  -db foobar -db test -alltables -dumpcompress 'gzip' 		       			     	       $DEBUG_CMD " && echo "Test  11: failure" && exit 11
echo "Test  11: ok ( $? )"

# test 12 , need args
eval "$BINARY  -db foobar -db test -alltables -chunksize 40        		       			     	       $DEBUG_CMD " && echo "Test  12: failure" && exit 12
echo "Test  12: ok ( $? )"

# test 13 , need args
eval "$BINARY  -db foobar -db test -alltables -port 4000 -pwd test1234 -user foobar  			     	       $DEBUG_CMD " && echo "Test  13: failure" && exit 13
echo "Test  13: ok ( $? )"

# test 14 , bad database
eval "$BINARY  -db foobar -db foobar_copy -alltables -port 4000 -pwd test1234 -user foobar  -guessprimarykey 	       $DEBUG_CMD " && echo "Test  14: failure" && exit 14
echo "Test  14: ok ( $? )"

# test 15 , bad table
eval "$BINARY  -db foobar -port 4000 -pwd test1234 -user foobar -table a_very_bad_table                      	       $DEBUG_CMD " && echo "Test  15: failure" && exit 15
echo "Test  15: ok ( $? )"

# test 16 , not a regular table
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db information_schema -table views   	       $DEBUG_CMD " && echo "Test  16: failure" && exit 16
echo "Test  16: ok ( $? )"

# test 17 , not a innodb table
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db mysql -table users                	       $DEBUG_CMD " && echo "Test  17: failure" && exit 17
echo "Test  17: ok ( $? )"

# test 18 , not a regular table
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db foobar -table client_report       	       $DEBUG_CMD " && echo "Test  18: failure" && exit 18
echo "Test  18: ok ( $? )"

# test 19 , compression not avaliable for cpy
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -table client_info -db foobar --dumpmode cpy  -dumpcompress zstd $DEBUG_CMD " && echo "Test  19: failure" && exit 19
echo "Test  19: ok ( $? )"

# test 20 , bad argument
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -table client_info -db foobar barfoo -guessprimarykey            $DEBUG_CMD " && echo "Test  20: failure" && exit 20
echo "Test  20: ok ( $? )"

# test 100  dump client_info ticket_tag sql insertsize 1 => count lines and compare with mysqldump
TMPDIR=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db foobar -guessprimarykey --dumpmode sql -dumpfile '${TMPDIR}/dump_%d_%t_%p%m%z' --dumpinsert simple  --dumpheader=false -insertsize 1 $( echo "$LIST_SMALL_TABLES"  | xargs -n1 printf -- '-table %s ' ) $DEBUG_CMD " || {  echo "Test 100: failure" ; exit 100 ; }
FAIL=0
for T in $LIST_SMALL_TABLES
do
    SQL_CNT=0
    for F in "${TMPDIR}/dump_foobar_${T}"_*.sql
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
    ${DCK_MYSQL_DUMP} -u foobar -ptest1234  --port 4000 -h 127.0.0.1 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact foobar "$T"  2>/dev/null  | grep -v '^$' >  "${TMPDIR}/mysqldump_foobar_${T}.sql" 2>/dev/null
done
for T in $LIST_SMALL_TABLES
do
    CNT_LINES_SRC=$(cat "${TMPDIR}/dump_foobar_${T}"_*.sql | LANG=C grep -c '^INSERT' )
    CNT_LINES_MDP=$(LANG=C grep -c '^INSERT' < "${TMPDIR}/mysqldump_foobar_${T}".sql )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_MDP" ]]
    then
	FAIL=$((FAIL+1))
    else
	DIFF_RES=$(mktemp "$TMPDIR/diff_XXXXXXX")
	LANG=C diff -a -u <(LANG=C  sort "${TMPDIR}/dump_foobar_${T}"_*.sql ) <(LANG=C sort "${TMPDIR}/mysqldump_foobar_${T}".sql )  > "$DIFF_RES"
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
    echo "info are in $TMPDIR"
    exit 100
fi
echo "Test 100: ok ( $? )"
rm -rf "$TMPDIR"

# test 110  dump whole database csv with no header => count lines
TMPDIR_T110=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db foobar -alltables -guessprimarykey --dumpmode csv --dumpheader=false -dumpfile '${TMPDIR_T110}/dump_%d_%t_%p%m%z' $DEBUG_CMD " || { echo "Test 110: failure" ; exit 110 ; }
FAIL=0
for T in $LIST_TABLES_CSV
do
    CSV_CNT=0
    for F in "${TMPDIR_T110}/dump_foobar_${T}"_*.csv
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
    echo "Test 110: failure ($FAIL)"
    echo "info are in $TMPDIR"
    exit 110
fi
echo "Test 110: ok ( $? )"

# test 111  dump whole database csv => count lines
TMPDIR=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db foobar -alltables -guessprimarykey --dumpmode csv -dumpfile '${TMPDIR}/dump_%d_%t_%p%m%z' $DEBUG_CMD " || { echo "Test 111: failure" ; exit 111 ; }
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
    echo "Test 111: failure ($FAIL)" && exit 111
fi
echo "Test 111: ok ( $? )"
rm -rf "$TMPDIR"

# test 112  dump whole database csv / zstd => count lines
TMPDIR=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db foobar -alltables -guessprimarykey --dumpmode csv -dumpfile '${TMPDIR}/dump_%d_%t_%p%m%z' -dumpcompress zstd $DEBUG_CMD " || { echo "Test 112: failure" ; exit 112 ; }
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
    echo "Test 112: failure ($FAIL)" && exit 112
fi
echo "Test 112: ok ( $? )"
rm -rf "$TMPDIR"

# test 121  dump whole database sql => count lines
TMPDIR_T121=$(mktemp -d )
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db foobar -alltables -guessprimarykey --dumpmode sql -dumpfile '${TMPDIR_T121}/dump_%d_%t_%p%m%z' --dumpinsert simple  --dumpheader=false -insertsize 1 $DEBUG_CMD " || {  echo "Test 121: failure" ; exit 121 ; }
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
    ${DCK_MYSQL_DUMP} -u foobar -ptest1234  --port 4000 -h 127.0.0.1 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact foobar "$T"  2>/dev/null  | grep -v '^$' >  "${TMPDIR_T121}/mysqldump_foobar_${T}.sql" 2>/dev/null
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
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db foobar -alltables -guessprimarykey --dumpmode sql -dumpfile '${TMPDIR}/dump_%d_%t_%p%m%z' -dumpcompress zstd -insertsize 1 $DEBUG_CMD " || { echo "Test 122: failure" ; exit 122 ; }
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
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db foobar -alltables -guessprimarykey --dumpmode cpy -dst-port=5000 -dst-user=foobar -dst-pwd=test1234                     $DEBUG_CMD " || { echo "Test 130: failure" ; exit 130 ; }
FAIL=0
for T in $LIST_TABLES
do
    CNT=$(${DCK_MYSQL}  -u foobar -ptest1234 --port 5000 -h 127.0.0.1 foobar -e "select count(*) as cnt from $T \G" 2>/dev/null | sed 's/^cnt: //p;d')
    if [[ "$CNT" -ne "$( eval "echo \$CNT_$T" )" ]]
    then
	FAIL=$((FAIL+1))
    fi
done
CNT_TAG_MATCH_U8=$(${DCK_MYSQL}  -u foobar -ptest1234 --port 5000 -h 127.0.0.1 foobar -e "select count(*) as cnt_match  from ticket_tag where label_hex_u8 = hex(cast(convert(label using utf8mb4)  as binary)) \G" 2>/dev/null | sed 's/^cnt_match: //p;d'  )
if [[ "$CNT_TAG_MATCH_U8" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+32))
fi
CNT_TAG_MATCH_L1=$(${DCK_MYSQL}  -u foobar -ptest1234 --port 5000 -h 127.0.0.1 foobar -e "select count(*) as cnt_match  from ticket_tag where label_hex_l1 = hex(cast(convert(label using latin1)  as binary)) \G" 2>/dev/null | sed 's/^cnt_match: //p;d'  )
if [[ "$CNT_TAG_MATCH_L1" -ne "$( eval "echo \$CNT_ticket_tag" )" ]]
then
    FAIL=$((FAIL+1024))
fi
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 130: failure ($FAIL)" && exit 130
fi
echo "Test 130: ok ( $? )"

# test 131  dump whole database csv => count lines
eval "$BINARY  -port 5000 -pwd test1234 -user foobar  -guessprimarykey -db foobar -alltables -guessprimarykey -dumpmode csv -dumpheader=false -dumpfile '${TMPDIR_T110}/dump_%d_copy_%t_%p%m%z' $DEBUG_CMD " || { echo "Test 131: failure" ; exit 131  ; }
FAIL=0
for T in $LIST_TABLES_CSV
do
    DIFF_RES=$(mktemp)
    CNT_LINES_SRC=$(cat "${TMPDIR_T110}/dump_foobar_${T}"_*.csv | wc -l )
    CNT_LINES_DST=$(cat "${TMPDIR_T110}/dump_foobar_copy_${T}"_*.csv | wc -l )
    if [[ "$CNT_LINES_SRC" -ne "$CNT_LINES_DST" ]]
    then
	FAIL=$((FAIL+1))
    else
	diff -u <( sort "${TMPDIR_T110}/dump_foobar_${T}"_*.csv ) <( sort "${TMPDIR_T110}/dump_foobar_copy_${T}"_*.csv )  > "$DIFF_RES"
	CNT_PLUS=$( grep -c '^+'  "$DIFF_RES" )
	CNT_MINUS=$( grep -c '^-'  "$DIFF_RES" )
	if [[ "$CNT_PLUS" -gt 0 || "$CNT_MINUS" -gt 0 ]]
	then
	    FAIL=$((FAIL+1))
	fi
    fi
done
if [[ "$FAIL" -gt 0 ]]
then
    echo "Test 131: failure ($FAIL)" && exit 131
fi
echo "Test 131: ok ( $? )"
# test 132  dump whole database sql => count lines and compare mysqldump from t121 and from copy db inserted by T130
eval "$BINARY  -port 5000 -pwd test1234 -user foobar  -guessprimarykey -db foobar -alltables -guessprimarykey --dumpmode sql -dumpfile '${TMPDIR_T121}/dump_%d_copy_%t_%p%m%z' --dumpinsert simple  --dumpheader=false -insertsize 1 $DEBUG_CMD " || {  echo "Test 132: failure" ; exit 132 ; }
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
    ${DCK_MYSQL_DUMP} -u foobar -ptest1234  --port 5000 -h 127.0.0.1 --skip-add-drop-table --skip-add-locks  --skip-disable-keys --no-create-info  --no-tablespaces --skip-extended-insert --compact foobar "$T"  2>/dev/null  | grep -v '^$' >  "${TMPDIR_T121}/mysqldump_foobar_copy_${T}.sql" 2>/dev/null
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

rm -rf "$TMPDIR_T110"
rm -rf "$TMPDIR_T121"
