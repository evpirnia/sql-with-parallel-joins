#!/bin/bash

echo "RUNNING TEST 1 ...................................................."
./test1-jane-1.pre
echo "* executing create books statement"
./run.sh test1-jane-1.cfg test1-jane-1.sql | sort > test1-jane-1.out
echo "* output from create books statement stored in test1-jane-1.out"
echo "* query nodes to check"
touch temp.txt
./test1-jane-1.post &> temp.txt
sed '/Warning/d' temp.txt &> temp2.txt
sed '/Connection to /d' temp2.txt &> test1-jane-1.post.out
rm temp.txt
rm temp2.txt
echo "* print output from querying nodes"
diff -s test1-jane-1.post.out test1-jane-1.post.exp
./test1-cleanup
