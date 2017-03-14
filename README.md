# SQL-with-Parallel-Joins
https://lipyeow.github.io/ics421s17/morea/queryproc/experience-hw3.html

Overview: This program modifies previous versions of the parallel SQL databases,
by adding the feature of performing SELECT-FROM-WHERE clauses involving joins
between two tables.<br />

===========================<br />

Inputs for runSQL: cluster.cfg & sqlfile.sql<br />

*runSQL*: Outputs the rows retrieved to the standard output on success or report
failure<br />

*Cluster.cfg*: Contains the access information for tha catalog DB<br />

*sqlfile.sql*: Contains the SQL query terminated by a semi-colon to be executed.<br />
The join query needs to be executed on multiple threads<br />

*run.sh*: Shell script to run the entire program, taking in both cluster.cfg and
sqlfile.sql<br />

===========================<br />

LOCAL MACHINE (macOS)

Install Homebrew:<br />
$ ```/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"```<br />

Install virtualbox & vagrant:<br />
$ ```brew cask install virtualbox```<br />
$ ```brew cask install vagrant```<br />
$ ```brew cask install vagrant-manager```<br />

Install python3 if not already on machine: <br />
$ ```brew install python3```<br />

============================

LOCAL MACHINE (Linux)<br />

Install virtualbox & vagrant:<br />
$ ```sudo apt install virtualbox```<br />
$ ```sudo apt install vagrant```<br />
$ ```sudo apt install vagrant-manager```<br />

Install python3 if not already on machine:<br />
$ ```sudo apt install python3```<br />

==========================

Install PyMySQL if not already on machine:<br />
$ ```pip3 install PyMySQL```<br />

Create a Directory for the Catalog and each Node:<br />
$ ```mkdir catalog```<br />
$ ```mkdir machine1```<br />
$ ```mkdir machine2```<br />

Clone all necessary files from the repo:<br />
1) test.py<br />
2) cluster.cfg<br />
3) sqlfile.sql<br />
4) csv.py<br />
5) run.sh<br />

Initialize a virtual machine in each directory:<br />
$ ```vagrant init ubuntu/xenial64```<br />
$ ```vagrant up```<br />
$ ```vagrant ssh```<br />

Change to the /vagrant directory of each virtual machine<br />
$ ```cd /vagrant```<br />

Open Vagrantfile in Each Directory:<br />
Replace line 25 with:<br />
  config.vm.network "forwarded_port", guest: 3306, host: 3306, auto_correct: true<br />
Replace line 29 with:<br />
  config.vm.network "private_network", ip: "ADDRESS_VALUE"<br />
Note: ADDRESS_VALUE depends on Directory:<br />
  /machine2, address_value = localhost_network.20<br />
  /machine1, address_value = localhost_network.10<br />
  /catalog, address_value = localhost_network.30<br />

Install MySQL in Each Directory:<br />
$ ```sudo apt-get install mysql-server```<br />
Note: Password for MySQL-server: password<br />
$ ```/usr/bin/mysql_secure_installation```<br />
Note: Respond No to everything but Remove test database and access to it, and Reload privilege tables<br />

Run the following command then comment out the bind-address in the catalog and each machine: <br />
$ ```sudo vim /etc/mysql/mysql.conf.d/mysqld.cnf```<br />
bind-address = 127.0.0.1<br />
Note: save and exit vim
$ ```sudo service mysql restart```<br />

Connect to MySQL in Each Directory:<br />
$ ```mysql -u root -p;```<br />
Enter password: 'password'<br />

Create a database in Each Directory: <br />
NOTE: If a user already exists then drop it:<br />
mysql> ```drop user 'username'```<br />
Create the database:<br />
mysql> ```create database TESTDB;```<br />

Create Remote Users in Each Directory:<br />
mysql> ```use TESTDB;```<br />
mysql> ```create user 'username';```<br />
mysql> ```grant all on TESTDB.* to username@'%' identified by 'password';```<br />
mysql> ```exit```<br />
$ ```exit```<br />

Create table for machine1 and insert some data:<br />
mysql> ```use TESTDB;```<br />
mysql> ```create table candy (name char(80), chocolate(4), rating int (2));```<br />
mysql> ```insert into candy values ('Sour Patch Kids', 'No', '5');```<br />
mysql> ```insert into candy values ('Mike n Ikes', 'No', '1');```<br />

Create table for machine2 and insert some data:<br />
mysql> ```use TESTDB;```<br />
mysql> ```create table movies (title char(80), released int(4), rating int (2));```<br />
mysql> ```insert into movies values ('Bad Boys 1', '1995', '4');```<br />
mysql> ```insert into movies values ('Bad Boys 2', '2003', '5');```<br />
mysql> ```insert into movies values ('Split', '2017', '5');```<br />

Insert the existing tables into dtables of the catalog machine:<br />
mysql> ```use TESTDB;```<br />
mysql> ```insert into dtables values ('candy', NULL, 'jdbc:mysql://192.168.10.10:3306/TESTDB', 'evelynp', 'netflix', NULL, '1', NULL, NULL, NULL);```<br />
mysql> ```insert into dtables values ('movies', NULL, 'jdbc:mysql://192.168.10.20:3306/TESTDB', 'blakela', 'hulu', NULL, '2', NULL, NULL, NULL);```<br />
Note: The nodeurl has the specific user's ip address (ie. .10 or .20). nodeid, nodeuser, nodepasswd also correspond with the specific user. <br />

*If continuous failure to connect to any of the databases, try this:*<br />
mysql> ```show grants for 'username'@'localhost';```<br />
mysql> ```flush privileges;```<br />
mysql> ```select * from mysql.user where user='username' \G;```<br />

*If The problem isn't solved, the last resort is to reset root privileges:*<br />
mysql> ```update mysql.user set Grant_priv='Y', Super_priv='Y' where User='root';```<br />

Run the script from your local host repo:<br />
$ ```./run.sh ./cluster.cfg ./sqlfile.sql```<br />
