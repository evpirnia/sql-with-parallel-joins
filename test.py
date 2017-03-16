#!/usr/bin/python3
import pymysql
import threading
import sys
from sys import argv

def runSQL(argv):
    # Define variables
    # hostname = ""
    # username = ""
    # passwd = ""
    # url = ""
    # port = ""
    # db = ""
    num = 0

    # Array of Nodes
    localnodes = []

    # Takes in Command Line Arguments for files
    fname, clustercfg, sqlfile = argv

    # Reads clustercfg file line by line for catalog information and localnode information
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
                        catalog.createCatalog()
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
                        localnodes.append(n)
                        catalog.updateCatalog(n)

    # Reads sqlfile, Remove Whitespace, Remove new lines, Parse contents on ';'
    # print("Reading sqlfile .........")
    # k = open(sqlfile, "r")
    # sqlcmds = list(filter(None, k.read().strip().replace("\n","").split(';')))
    # for s in sqlcmds:
    #     print()
    #     print(s)
    # print("end reading sqlfile .........")

    # Read Catalog Contents and Create Nodes
    # cmd = "select * from dtables"
    # try:
    #     connect = pymysql.connect(catalog.hostname, catalog.username, catalog.passwd, catalog.db)
    #     cur = connect.cursor()
    #     cur.execute(cmd)
    #     data = cur.fetchall()
    #     for d in data:
    #         tname = d[0]
    #         nodedriver = d[1]
    #         nodeurl = d[2]
    #         nodeuser = d[3]
    #         nodepasswd = d[4]
    #         nodeid = d[6]
    #         hostname = str(nodeurl).split("/", 2)[2].split(":")[0]
    #         port = str(nodeurl).split("/", 2)[2].split(":")[1].split("/")[0]
    #         db = str(nodeurl).split("/", 2)[2].split(":")[1].split("/")[1]
    #         print(tname, nodeurl, nodeuser, nodepasswd, nodeid, hostname, port, db)
    #         t = Node(hostname, nodeuser, db, nodeid, nodeurl, port)
    #         nodes.append(t)
    #     connect.close()
    # except pymysql.OperationalError:
    #     print("[", catalog.url, "]:", sqlfile, " failed.")

    # run sql commands via threading
    # threads = []
    # for s in sqlcmds:
    #     for n in nodes:
    #         threads.append(NodeThread(n, s, sqlfile).start())

    k.close()

def runCommands(n, s, sqlfile):
    try:
        connect = pymysql.connect(n.hostname, n.username, n.passwd, n.db)
        cur = connect.cursor()
        cur.execute(s)
        data = cur.fetchall()
        for d in data:
            print(d)
        print("[", n.url, "]:", sqlfile, " success.")
        connect.close()
    except pymysql.OperationalError:
        print("[", n.url, "]:", sqlfile, " failed.")
    except pymysql.ProgrammingError:
        print("[", n.url, "]:", sqlfile, " failed.")

class NodeThread(threading.Thread):
    def __init__(self, node, cmd, sqlfile):
        threading.Thread.__init__(self)
        self.node = node
        self.cmd = cmd
        self.sqlfile = sqlfile
    def run(self):
        runCommands(self.node, self.cmd, self.sqlfile)

class Catalog:
    'Base Class for Catalog'
    def __init__(self, hostname, username, passwd, db, url, port):
        self.hostname = hostname.replace(" ", "")
        self.username = username.replace(" ", "")
        self.passwd = passwd.replace(" ", "")
        self.db = db.replace(" ", "")
        self.url = url
    def displayCatalogInfo(self):
        print("Hostname: ", self.hostname, " Username: ", self.username, " Passwd: ", self.passwd, " DB: ", self.db)
    def updateCatalog(self, n):
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
                    cur.execute("""INSERT INTO dtables VALUES (%s, NULL, %s, %s, %s, NULL, %s, NULL, NULL, NULL)""", (g, n.url, n.username, n.passwd, n.num))
                    connect.commit()
                connect.close()
            except pymysql.InternalError:
                print("Error")
            except pymysql.OperationalError:
                print("Error")
    def createCatalog(self):
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
    def displayNode(self):
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
