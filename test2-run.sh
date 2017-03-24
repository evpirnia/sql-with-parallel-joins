#!/bin/bash

echo "RUNNING TEST 2 ...................................................."
./test2-jane-1.pre &> garbage.txt
echo "* executing insert statement int books table"
./run.sh test2-jane-1.cfg test2-jane-1.sql | sort > test2-jane-1.out
echo "* output from insert stored in test2-jane-1.out"
echo "* query nodes to check and store in post.out"
touch temp.txt
./test2-jane-1.post &> temp.txt
sed '/Warning/d' temp.txt &> temp2.txt
sed '/Connection to /d' temp2.txt &> test2-jane-1.post.out
rm temp.txt
rm temp2.txt
echo "* print output from querying nodes"
diff -s test2-jane-1.post.out test2-jane-1.post.out.exp
rm garbage.txt
