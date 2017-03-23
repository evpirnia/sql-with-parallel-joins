#!/bin/bash

echo "RUNNING TEST 4 ...................................................."
./test4-jane-1.pre
echo "* querying the distributed tables using SQL joining two tables"
./run.sh test4-jane-1.cfg test4-jane-1.sql | sort > test4-jane-1.out
echo "* output from insert stored in test4-jane-1.out"
./test4-jane-1.post &> temp.txt
sed '/Warning/d' temp.txt &> temp2.txt
sed '/Connection to /d' temp2.txt &> test4-jane-1.post.out
rm temp.txt
rm temp2.txt
echo "* print output from querying nodes"
diff -s test4-jane-1.post.out test4-jane-1.post.out.exp
./test1-cleanup
