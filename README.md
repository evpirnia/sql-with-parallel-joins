# SQL-with-Parallel-Joins
https://lipyeow.github.io/ics421s17/morea/queryproc/experience-hw3.html

Overview: This program modifies previous versions of the parallel SQL databases,
by adding the feature of performing SELECT-FROM-WHERE clauses involving joins
between two tables.<br />

Inputs for runSQL: cluster.cfg & sqlfile.sql<br />

Cluster.cfg: Contains the access information for tha catalog DB<br />

sqlfile.sql: Contains the SQL query terminated by a semi-colon to be executed.<br />
  *The join query needs to be executed on multiple threads*<br />

run.sql: Outputs the rows retrieved to the standard output on success or report
failure<br />
