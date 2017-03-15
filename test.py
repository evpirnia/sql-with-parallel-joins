#!/usr/bin/python3
import pymysql
import threading
import sys
from sys import argv

def runSQL(argv):
    # Define variables
    hostname = ""
    username = ""
    passwd = ""
    url = ""
    port = ""
    db = ""
    num = 0

    # List of Nodes
    nodes = []

    # Takes in Command Line Arguments for files
    fname, clustercfg, ddlfile = argv

    # Reads clustercfg file line by line for catalog information
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
                        catalog = Catalog(hostname, username, passwd, db, url)
                        catalog.createCatalog()

    # Reads ddlfile, Remove Whitespace, Remove new lines, Parse contents on ';'
    k = open(ddlfile, "r")
    sqlcmds = list(filter(None, k.read().strip().replace("\n","").split(';')))

    # Read Catalog Contents and Create Nodes
    cmd = "select * from dtables"
    try:
        connect = pymysql.connect(catalog.hostname, catalog.username, catalog.passwd, catalog.db)
        cur = connect.cursor()
        cur.execute(cmd)
        data = cur.fetchall()
        for d in data:
            tname = d[0]
            nodedriver = d[1]
            nodeurl = d[2]
            nodeuser = d[3]
            nodepasswd = d[4]
            nodeid = d[6]
            hostname = str(nodeurl).split("/", 2)[2].split(":")[0]
            port = str(nodeurl).split("/", 2)[2].split(":")[1].split("/")[0]
            db = str(nodeurl).split("/", 2)[2].split(":")[1].split("/")[1]
            print(tname, nodeurl, nodeuser, nodepasswd, nodeid, hostname, port, db)
            t = Node(hostname, nodeuser, db, nodeid, nodeurl, port)
            nodes.append(t)
        connect.close()
    except pymysql.OperationalError:
        print("[", catalog.url, "]:", ddlfile, " failed.")

    # run sql commands via threading
    threads = []
    for s in sqlcmds:
        for n in nodes:
            threads.append(NodeThread(n, s, ddlfile).start())

    k.close()

def runCommands(n, s, ddlfile):
    try:
        connect = pymysql.connect(n.hostname, n.username, n.passwd, n.db)
        cur = connect.cursor()
        cur.execute(s)
        data = cur.fetchall()
        for d in data:
            print(d)
        print("[", n.url, "]:", ddlfile, " success.")
        connect.close()
    except pymysql.OperationalError:
        print("[", n.url, "]:", ddlfile, " failed.")
    except pymysql.ProgrammingError:
        print("[", n.url, "]:", ddlfile, " failed.")

class NodeThread(threading.Thread):
    def __init__(self, node, cmd, ddlfile):
        threading.Thread.__init__(self)
        self.node = node
        self.cmd = cmd
        self.ddlfile = ddlfile
    def run(self):
        runCommands(self.node, self.cmd, self.ddlfile)

class Catalog:
    'Base Class for Catalog'
    def __init__(self, hostname, username, passwd, db, url):
        self.hostname = hostname.replace(" ", "")
        self.username = username.replace(" ", "")
        self.passwd = passwd.replace(" ", "")
        self.db = db.replace(" ", "")
        self.url = url
    def displayCatalogInfo(self):
        print("Hostname: ", self.hostname, " Username: ", self.username, " Passwd: ", self.passwd, " DB: ", self.db)
    def updateCatalog(self, table, n):
        try:
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            cur.execute("""INSERT INTO dtables VALUES (%s, NULL, %s, %s, %s, NULL, %s, NULL, NULL, NULL)""", (table, n.url, n.username, n.passwd, n.num))
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

runSQL(argv)
