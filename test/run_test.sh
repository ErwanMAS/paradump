#!/bin/bash

cd "$(dirname $0)"

BINARY=../src/paradump


# test  1 , need args
$BINARY                                                                                                >/dev/null 2>&1 && echo "Test  1: failure" && exit  1
echo "Test  1: ok ( $? )"

# test  2 , need args
$BINARY  -db foobar             		   	     		       			       >/dev/null 2>&1 && echo "Test  2: failure" && exit  2
echo "Test  2: ok ( $? )"

# test  3 , need args
$BINARY  -db foobar -alltables  		   	     		       			       >/dev/null 2>&1 && echo "Test  3: failure" && exit  3
echo "Test  3: ok ( $? )"

# test  4 , need args
$BINARY  -db foobar -alltables  		   	     		       			       >/dev/null 2>&1 && echo "Test  4: failure" && exit  4
echo "Test  4: ok ( $? )"

# test  5 , need args
$BINARY  -db foobar -alltables  -table client_info 	     		       			       >/dev/null 2>&1 && echo "Test  5: failure" && exit  5
echo "Test  5: ok ( $? )"

# test  6 , need args
$BINARY  -table client_info            		   	     		       			       >/dev/null 2>&1 && echo "Test  6: failure" && exit  6
echo "Test  6: ok ( $? )"

# test  7 , need args
$BINARY  -db foobar -db test -table client_info    	     		       			       >/dev/null 2>&1 && echo "Test  7: failure" && exit  7
echo "Test  7: ok ( $? )"

# test  8 , need args
$BINARY  -db foobar -db test                       	     		       			       >/dev/null 2>&1 && echo "Test  8: failure" && exit  8
echo "Test  8: ok ( $? )"

# test  9 , need args
$BINARY  -db foobar -db test -alltables -dumpfile ""         		       			       >/dev/null 2>&1 && echo "Test  9: failure" && exit  9
echo "Test  9: ok ( $? )"

# test 10 , need args
$BINARY  -db foobar -db test -alltables -dumpmode "ods"      		       			       >/dev/null 2>&1 && echo "Test 10: failure" && exit 10
echo "Test 10: ok ( $? )"

# test 11 , need args
$BINARY  -db foobar -db test -alltables -dumpcompress "gzip" 		       			       >/dev/null 2>&1 && echo "Test 11: failure" && exit 11
echo "Test 11: ok ( $? )"

# test 12 , need args
$BINARY  -db foobar -db test -alltables -chunksize 40        		       			       >/dev/null 2>&1 && echo "Test 12: failure" && exit 12
echo "Test 12: ok ( $? )"

# test 13 , need args
$BINARY  -db foobar -db test -alltables -port 4000 -pwd test1234 -user foobar  			       >/dev/null 2>&1 && echo "Test 13: failure" && exit 13
echo "Test 13: ok ( $? )"

# test 14 , bad database
$BINARY  -db foobar -db foobar_copy -alltables -port 4000 -pwd test1234 -user foobar  -guessprimarykey >/dev/null 2>&1 && echo "Test 14: failure" && exit 14
echo "Test 14: ok ( $? )"


