#!/usr/bin/python3
import pymysql
import threading
import sys
from sys import argv

def runSQL(argv):
    # Takes in Command Line Arguments for files
    fname, clustercfg, sqlfile = argv

    # Reads clustercfg file line by line for catalog information and localnode information
    localnodes = readClustercfg(clustercfg)

    # Count Duplicate Tables
    duplicatetables = getDuplicates(localnodes)

    mergeDuplicates(localnodes)

    # Execute commands read in from edited sqlfile
    # data = []
    # k = open(sqlfile, "r")
    # sqlcmds = list(filter(None, k.read().strip().replace("\n"," ").split(';')))
    # for s in sqlcmds:
    #
    #     # check if cmd involes tables that are partitioned on each node
    #     s_new = checkFrom(s, duplicatetables)
    #
    #     # if sql command involes tables that are partitioned, use the temp tables
    #     if s.find(s_new) == -1:
    #         data = runCommand(localnodes[0], s_new, sqlfile, 1)
    #
    #     for n in localnodes:
    #         data2 = runCommand(n, s, sqlfile, 0)
    #         for d2 in data2:
    #             if d2 not in data:
    #                 data.append(d2)
    #
    # for d in data:
    #     print(d)
    #
    # # delete all temp tables created
    # for d in duplicatetables:
    #     cleanupMerge(localnodes[0], d)

    # run sql commands via threading
    threads = []
    k = open(sqlfile, "r")
    sqlcmds = list(filter(None, k.read().strip().replace("\n"," ").split(';')))
    k.close()
    for s in sqlcmds:
        # check if cmd involes tables that are partitioned on each node
        s_new = checkFrom(s, duplicatetables)
        # if sql command involes tables that are partitioned, use the temp tables
        if s.find(s_new) == -1:
            threads.append(NodeThread(localnodes[0], s_new, sqlfile, 1).start())
            # still try to run the sql cmd on the nodes but do not let them print output
            for n in localnodes:
                threads.append(NodeThread(n, s, sqlfile, 0).start())
        else:
            for n in localnodes:
                threads.append(NodeThread(n, s, sqlfile, 1).start())


    # delete all temp tables created
    for d in duplicatetables:
        cleanupMerge(localnodes[0], d)

def checkFrom(sqlcmd, duplicates):
    for d in duplicates:
        if sqlcmd.find(d) > -1:
            sqlcmd = sqlcmd.replace(d, "temp%s" % (d))
    return sqlcmd

def mergeDuplicates(localnodes):
    for n in localnodes:
        if n.num == 1:
            top = n
        else:
            for toptables in top.getTables():
                for ntables in n.getTables():
                    if toptables.find(ntables) > -1:
                        runMerge(n, top, ntables)

def runMerge(n, t, tablename):
    try:
        # connect to both databases
        nconnect = pymysql.connect(n.hostname, n.username, n.passwd, n.db)
        tconnect = pymysql.connect(t.hostname, t.username, t.passwd, t.db)
        ncur = nconnect.cursor()
        tcur = tconnect.cursor()
        cmd = "SELECT * from %s" % (tablename)
        ncur.execute(cmd)
        temp = ncur.fetchall()
        cmd = "CREATE TABLE temp%s (select * from %s)" % (tablename, tablename)
        tcur.execute(cmd)
        # add column NODE to reference which nodes were merged
        cmd = "ALTER TABLE temp%s ADD nodenum VARCHAR(32)" % (tablename)
        tcur.execute(cmd)
        cmd = "UPDATE temp%s SET nodenum=%s" % (tablename, t.num)
        tcur.execute(cmd)
        for t in temp:
            if str(t).find("datetime.date") > -1:
                index = str(t).find("datetime.date")
                datetime = str(t)[(index+14):].replace("))","").replace(" ", "").split(",")
                year = datetime[0]
                month = datetime[1]
                day = datetime[2]
                t = str(t)[:index] + "'" + year + "-" + month + "-" + day + "'"+ str(t)[(index+25):]
            t = str(t)[:len(str(t))-1] + ", " + str(n.num) + str(t)[len(str(t))-1:]
            cmd = "INSERT INTO temp%s values %s" % (tablename, t)
            tcur.execute(cmd)
            tconnect.commit()
        nconnect.close()
        tconnect.close()
    except pymysql.Error:
        print("Error")

def cleanupMerge(t, tablename):
    try:
        tconnect = pymysql.connect(t.hostname, t.username, t.passwd, t.db)
        tcur = tconnect.cursor()
        cmd = "DROP TABLE temp%s" % (tablename)
        tcur.execute(cmd)
        tconnect.close()
    except pymysql.Error:
        print("Error")

def getDuplicates(localnodes):
    temp = []
    alltables = []
    for nodes in localnodes:
        for table in nodes.getTables():
            alltables.append(table)
    for table in alltables:
        if alltables.count(table) >= 2:
            if table not in temp:
                temp.append(table)
    return temp

def readClustercfg(clustercfg):
    num = 0
    temp2 = []
    k = open(clustercfg, "r")
    with open(clustercfg) as fin:
        for line in fin:
            if line.strip():
                temp = line.strip().split("=")
                if temp[0].split(".")[0].find("catalog") > -1:
                    if temp[0].split(".")[1].find("driver") > -1:
                        pass
                    elif temp[0].split(".")[1].find("hostname") > -1:
                        url = temp[1]
                        hostname = temp[1].split("/", 2)[2].split(":")[0]
                        port = temp[1].split("/", 2)[2].split(":")[1].split("/")[0]
                        db = temp[1].split("/", 2)[2].split(":")[1].split("/")[1]
                    elif temp[0].split(".")[1].find("username") > -1:
                        username = temp[1]
                    elif temp[0].split(".")[1].find("passwd") > -1:
                        passwd = temp[1]
                        catalog = Catalog(hostname, username, passwd, db, url, port)
                        catalog.create()
                if temp[0].split(".")[0].find("localnode") > -1:
                    if temp[0].split(".")[1].find("driver") > -1:
                        num = num + 1
                    elif temp[0].split(".")[1].find("hostname") > -1:
                        url = temp[1]
                        hostname = temp[1].split("/", 2)[2].split(":")[0]
                        port = temp[1].split("/", 2)[2].split(":")[1].split("/")[0]
                        db = temp[1].split("/", 2)[2].split(":")[1].split("/")[1]
                    elif temp[0].split(".")[1].find("username") > -1:
                        username = temp[1]
                    elif temp[0].split(".")[1].find("passwd") > -1:
                        passwd = temp[1]
                        n = Node(hostname, username, passwd, db, num, url, port)
                        temp2.append(n)
                        catalog.add(n)
    return temp2

def runCommand(n, s, sqlfile, mood):
    retval = []
    try:
        connect = pymysql.connect(n.hostname, n.username, n.passwd, n.db)
        cur = connect.cursor()
        cur.execute(s)
        temp = cur.fetchall()
        for t in temp:
            retval.append(t)
        connect.close()
    except pymysql.Error:
        print("Error")
    if mood == 0:
        if len(retval) > 0:
            print("[", n.url, "]:", sqlfile, " success.")
        else:
            print("[", n.url, "]:", sqlfile, " failed.")
    else:
        for r in retval:
            print(r)

class NodeThread(threading.Thread):
    def __init__(self, node, cmd, sqlfile, mood):
        threading.Thread.__init__(self)
        self.node = node
        self.cmd = cmd
        self.sqlfile = sqlfile
        self.mood = mood
    def run(self):
        runCommand(self.node, self.cmd, self.sqlfile, self.mood)

class Catalog:
    'Base Class for Catalog'
    def __init__(self, hostname, username, passwd, db, url, port):
        self.hostname = hostname.replace(" ", "")
        self.username = username.replace(" ", "")
        self.passwd = passwd.replace(" ", "")
        self.db = db.replace(" ", "")
        self.url = url
    def display(self):
        print("Hostname: ", self.hostname, " Username: ", self.username, " Passwd: ", self.passwd, " DB: ", self.db)
    def add(self, n):
        nodetables = n.getTables()
        for g in nodetables:
            try:
                connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
                cur = connect.cursor()
                # checking if table already exists
                cmd = "select count(tname) from dtables where tname='%s' and nodeurl='%s'" % (g, n.url)
                cur.execute(cmd)
                data = cur.fetchone()[0]
                # if data > 0 then tname already exists
                if int(data) > 0:
                    # update statement
                    partmtd = 1
                    if(g.find("sailors") > -1):
                        partcol = 'boat'
                        if(n.num == 1):
                            param1 = 0
                            param2 = 1
                        else:
                            param1 = 2
                            param2 = 5
                    if(g.find("reserves") > -1):
                        partcol = 'id'
                        if(n.num == 1):
                            param1 = 0
                            param2 = 4
                        else:
                            param1 = 4
                            param2 = 10
                    cmd = "UPDATE dtables SET tname='%s', nodedriver=NULL, nodeurl='%s', nodeuser='%s', nodepasswd='%s', partmtd='%s', nodeid='%s', partcol='%s', partparam1='%s', partparam2='%s' WHERE tname='%s' and nodeid='%s'" % (g, n.url, n.username, n.passwd, partmtd, n.num, partcol, param1, param2, g, n.num)
                    cur.execute(cmd)
                    connect.commit()
                else:
                    # insert statement
                    partmtd = 1
                    if(g.find("sailors") > -1):
                        partcol = 'boat'
                        if(n.num == 1):
                            param1 = 0
                            param2 = 1
                        else:
                            param1 = 2
                            param2 = 5
                    if(g.find("reserves") > -1):
                        partcol = 'id'
                        if(n.num == 1):
                            param1 = 0
                            param2 = 4
                        else:
                            param1 = 4
                            param2 = 10
                    cmd = "INSERT INTO dtables VALUES ('%s', NULL, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (g, n.url, n.username, n.passwd, partmtd, n.num, partcol, param1, param2)
                    cur.execute(cmd)
                    connect.commit()
                connect.close()
            except pymysql.InternalError:
                print("Error")
            except pymysql.OperationalError:
                print("Error")
    def create(self):
        try:
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            cur.execute("""CREATE TABLE dtables (tname VARCHAR(32), nodedriver VARCHAR(64), nodeurl VARCHAR(128), nodeuser VARCHAR(16), nodepasswd VARCHAR(16), partmtd INT, nodeid INT, partcol VARCHAR(32), partparam1 VARCHAR(32), partparam2 VARCHAR(32))""")
            connect.close()
        except pymysql.InternalError:
            pass

class Node:
    'Base Class for Nodes'
    def __init__(self, hostname, username, passwd, db, num, url, port):
        self.hostname = hostname.replace(" ", "")
        self.username = username.replace(" ", "")
        self.passwd = passwd.replace(" ", "")
        self.db = db.replace(" ", "")
        self.num = num
        self.url = url.replace(" ", "")
        self.port = port.replace(" ", "")
    def display(self):
        print("Hostname: ", self.hostname, " Username: ", self.username, " Passwd: ", self.passwd, " DB: ", self.db, " Num: ", self.num, " Url: ", self.url, " Port: ", self.port)
    def getTables(self):
        temp = []
        try:
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            cmd = "SELECT table_name FROM information_schema.tables where table_schema='%s'" % (self.db)
            cur.execute(cmd)
            data = cur.fetchall()
            for d in data:
                temp.append(d[0])
            connect.close()
        except pymysql.InternalError:
            pass
        return temp

runSQL(argv)
