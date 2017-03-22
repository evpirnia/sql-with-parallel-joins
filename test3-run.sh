#!/bin/bash

echo "RUNNING TEST 3 ...................................................."
./test3-jane-1.pre
echo "* querying distributed tables using SQL on a single table"
./run.sh test3-jane-1.cfg test3-jane-1.sql | sort > test3-jane-1.out
echo "* output from insert stored in test3-jane-1.out"
./test3-jane-1.post &> temp.txt
sed '/Warning/d' temp.txt &> temp2.txt
sed '/Connection to /d' temp2.txt &> test3-jane-1.post.out
rm temp.txt
rm temp2.txt
echo "* print output from querying nodes"
diff -s test3-jane-1.post.out test3-jane-1.post.out.exp
./test1-cleanup
