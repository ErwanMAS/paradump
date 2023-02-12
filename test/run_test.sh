#!/bin/bash

cd "$(dirname $0)"

BINARY=../src/paradump

DEBUG_CMD=">/dev/null 2>&1"

if [ "$1" = "--debug" ]
then
    set -x
    DEBUG_CMD=""
fi

# test  1 , need args
eval "$BINARY                                                                                                $DEBUG_CMD " && echo "Test  1: failure" && exit  1
echo "Test  1: ok ( $? )"

# test  2 , need args
eval "$BINARY  -db foobar             		   	     		       			             $DEBUG_CMD " && echo "Test  2: failure" && exit  2
echo "Test  2: ok ( $? )"

# test  3 , need args
eval "$BINARY  -db foobar -alltables  		   	     		       			             $DEBUG_CMD " && echo "Test  3: failure" && exit  3
echo "Test  3: ok ( $? )"

# test  4 , need args
eval "$BINARY  -db foobar -alltables --port 4000 		  	       			             $DEBUG_CMD " && echo "Test  4: failure" && exit  4
echo "Test  4: ok ( $? )"

# test  5 , need args
eval "$BINARY  -db foobar -alltables  -table client_info 	     		       			     $DEBUG_CMD " && echo "Test  5: failure" && exit  5
echo "Test  5: ok ( $? )"

# test  6 , need args
eval "$BINARY  -table client_info            		   	     		       			     $DEBUG_CMD " && echo "Test  6: failure" && exit  6
echo "Test  6: ok ( $? )"

# test  7 , need args
eval "$BINARY  -db foobar -db test -table client_info    	     		       			     $DEBUG_CMD " && echo "Test  7: failure" && exit  7
echo "Test  7: ok ( $? )"

# test  8 , need args
eval "$BINARY  -db foobar -db test                       	     		       			     $DEBUG_CMD " && echo "Test  8: failure" && exit  8
echo "Test  8: ok ( $? )"

# test  9 , need args
eval "$BINARY  -db foobar -db test -alltables -dumpfile ''         		       			     $DEBUG_CMD " && echo "Test  9: failure" && exit  9
echo "Test  9: ok ( $? )"

# test 10 , need args
eval "$BINARY  -db foobar -db test -alltables -dumpmode 'ods'      		       			     $DEBUG_CMD " && echo "Test 10: failure" && exit 10
echo "Test 10: ok ( $? )"

# test 11 , need args
eval "$BINARY  -db foobar -db test -alltables -dumpcompress 'gzip' 		       			     $DEBUG_CMD " && echo "Test 11: failure" && exit 11
echo "Test 11: ok ( $? )"

# test 12 , need args
eval "$BINARY  -db foobar -db test -alltables -chunksize 40        		       			     $DEBUG_CMD " && echo "Test 12: failure" && exit 12
echo "Test 12: ok ( $? )"

# test 13 , need args
eval "$BINARY  -db foobar -db test -alltables -port 4000 -pwd test1234 -user foobar  			     $DEBUG_CMD " && echo "Test 13: failure" && exit 13
echo "Test 13: ok ( $? )"

# test 14 , bad database
eval "$BINARY  -db foobar -db foobar_copy -alltables -port 4000 -pwd test1234 -user foobar  -guessprimarykey $DEBUG_CMD " && echo "Test 14: failure" && exit 14
echo "Test 14: ok ( $? )"

# test 15 , bad table
eval "$BINARY  -db foobar -port 4000 -pwd test1234 -user foobar -table a_very_bad_table                      $DEBUG_CMD " && echo "Test 15: failure" && exit 15
echo "Test 15: ok ( $? )"

# test 16 , not a regular table
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db information_schema -table views   $DEBUG_CMD " && echo "Test 16: failure" && exit 16
echo "Test 16: ok ( $? )"

# test 17 , not a innodb table
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db mysql -table users                $DEBUG_CMD " && echo "Test 17: failure" && exit 17
echo "Test 17: ok ( $? )"

# test 18 , not a regular table
eval "$BINARY  -port 4000 -pwd test1234 -user foobar  -guessprimarykey -db foobar -table client_report       $DEBUG_CMD " && echo "Test 18: failure" && exit 18
echo "Test 18: ok ( $? )"


