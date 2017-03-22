#!/usr/bin/python3
import pymysql
import threading
import sys
from sys import argv
import csv

def runSQL(argv):
    catalog = Catalog("", "", "", "", -1, "", "")

    # Takes in Command Line Arguments for files
    fname, clustercfg, sqlfile = argv
    # Reads clustercfg file line by line for catalog information and localnode information
    localnodes = []
    partitionmtd = []
    allnodes = readClustercfg(clustercfg, catalog)

    for n in allnodes:
        if(str(n).find("Node") > -1):
            localnodes.append(n)
        else:
            partitionmtd.append(n)

    # if clustercfg only has catalog information, read the catalog and create the localnodes list
    if not localnodes:
        localnodes = catalog.read()
        if not partitionmtd:
            # clustercfg only has catalog information, read catalog
            readsql(sqlfile, localnodes, catalog)
        else:
            # clustercfg only has catalog and partition info
            readcsv(partitionmtd, sqlfile, localnodes, catalog)
    else:
        readsql(sqlfile, localnodes, catalog)

def readcsv(partitionmtd, sqlfile, localnodes, catalog):
    csvcontents = []
    with open(sqlfile) as c:
        filtered = (line.replace('\n','') for line in c)
        reader = csv.reader(filtered, delimiter=',')
        header = next(reader)
        for row in reader:
            if any(field.strip() for field in row):
                csvcontents.append(row)

    # determine unique connections
    connections = catalog.getuniqueurl(localnodes)

    partmtd = partitionmtd[0][1]
    # all csv added to table
    if partmtd == 0:
        tname = partitionmtd[0][0]
        catalog.insert0(healder, connections, csvcontents, tname)
    # numnodes in catalog relation and the number of partitons in the config files must be the same
    # update nodes accordingly
    elif partmtd == 1:
        # determine number of nodes from catalog
        tname = partitionmtd[0][0]
        numnodes = partitionmtd[0][2]
        catalogNodes = catalog.countNodes(tname)
        if(int(catalogNodes) != int(numnodes)):
            print("Error")
        else:
            for m in partitionmtd:
                catalog.insert1(header, connections, csvcontents, m, m[0])
    # only values that meet hash function are added to table
    elif partmtd == 2:
        for m in mtd2info:
            catalog.insert2(header, connections, csvcontents, m, m[0])

def readsql(sql, localnodes, catalog):
    threads = []
    k = open(sql, "r")
    sqlcmds = list(filter(None, k.read().strip().replace("\n"," ").split(';')))
    k.close()
    for s in sqlcmds:
        smoo = getSmoo(s.split(" ")[0])
        # create command
        if smoo == 0:
            # update catalog is false
            printcatmsg = 0
            table = s.lower().split("table ")[1].split("(")[0]
            for n in localnodes:
                t = NodeThread(n, s, sql, 2, 0)
                threads.append(t)
                t.start()
                t.join()
                # threads.append(NodeThread(n, s, sql, 2, updatecatalog).start())
            for t in threads:
                if t.updatecatalog == 1:
                    printcatmsg = 1
                    catalog.update(table, t.node)
            if printcatmsg == 1:
                print("[" + catalog.url + "]: catalog updated")
            else:
                print("[" + catalog.url + "]: catalog not updated")

        # select command
        elif smoo == 1:
            # Count Duplicate Tables
            duplicatetables = getDuplicates(localnodes)
            if duplicatetables:
                mergeDuplicates(localnodes)
            # check if cmd involes tables that are partitioned on each node
            s_new = checkFrom(s, duplicatetables)
            # if sql command involes tables that are partitioned, use the temp tables
            if s.find(s_new) == -1:
                threads.append(NodeThread(localnodes[0], s_new, sql, -1, 0).start())
                # still try to run the sql cmd on the nodes but do not let them print output
                for n in localnodes:
                    threads.append(NodeThread(n, s, sql, 0, 0).start())
            else:
                for n in localnodes:
                    threads.append(NodeThread(n, s, sql, 1, 0).start())
            # delete all temp tables created
            for d in duplicatetables:
                cleanupMerge(localnodes[0], d)
        else:
            print("Invalid SQL File Command: ", getSmoo(s.split(" ")[0]))

def getSmoo(sqlcmdfirst):
    if sqlcmdfirst.lower().find("create") > -1:
        return 0
    elif sqlcmdfirst.lower().find("select") > -1:
        return 1
    else:
        # assume csv contents
        return -1

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
        pass

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

def readClustercfg(clustercfg, catalog):
    num = 0
    partmtd = -1
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
                        catalog.initialize(hostname, username, passwd, db, -1, url, port)
                        catalog.create()
                        # temp2.append(catalog)
                elif temp[0].split(".")[0].find("localnode") > -1:
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
                        tables = n.getTables()
                        for t in tables:
                            catalog.update(t, n)
                elif temp[0].find("tablename") > -1:
                    tname = temp[1]
                elif temp[0].find("partition.method") > -1:
                    if temp[1].find("notpartition") > -1:
                        partmtd = 0
                        temp = []
                        temp.append(tname)
                        temp.append(partmtd)
                        temp2.append(temp)
                    elif temp[1].find("range") > -1:
                        partmtd = 1
                    elif temp[1].find("hash") > -1:
                        partmtd = 2
                elif partmtd == 1:
                    if temp[0].find("numnodes") > -1:
                        numnodes = temp[1]
                    elif temp[0].find(".column") > -1:
                        column = temp[1]
                    elif temp[0].find(".param1") > -1:
                        param1 = temp[1]
                    elif temp[0].find(".param2") > -1:
                        param2 = temp[1]
                        nodenum = temp[0].split(".")[1].replace("node","")
                        temp = []
                        temp.append(tname)
                        temp.append(partmtd)
                        temp.append(numnodes)
                        temp.append(nodenum)
                        temp.append(column)
                        temp.append(param1)
                        temp.append(param2)
                        temp2.append(temp)
                elif partmtd == 2:
                    if temp[0].find(".column") > -1:
                        column = temp[1]
                    elif temp[0].find(".param1") > -1:
                        param1 = temp[1]
                        temp = []
                        temp.append(tname)
                        temp.append(partmtd)
                        temp.append(column)
                        temp.append(param1)
                        temp2.append(temp)

    return temp2

def runCommand(n, s, sqlfile, moo, updatecatalog):
    updatecatalog = 0
    retval = []
    try:
        connect = pymysql.connect(n.hostname, n.username, n.passwd, n.db)
        cur = connect.cursor()
        cur.execute(s)
        temp = cur.fetchall()
        for t in temp:
            retval.append(t)
        connect.close()
    except pymysql.ProgrammingError:
        print("[", n.url, "]:", sqlfile, " failed.")
        return updatecatalog
    except pymysql.InternalError:
        print("[", n.url, "]:", sqlfile, " failed.")
        return updatecatalog
    if moo == -1:
        if len(retval) > 0:
            getOutput(retval)
    elif moo == 0:
        # partition select for all nodes
        if len(retval) > 0:
            print("[", n.url, "]:", sqlfile, " success.")
            updatecatalog = 1
        else:
            print("[", n.url, "]:", sqlfile, " failed.")
    elif moo == 2:
        #create
        print("2[", n.url, "]:", sqlfile, " success.")
        updatecatalog = 1
    else:
        # nonpartitioned select
        if len(retval) > 0:
            print("[", n.url, "]:", sqlfile, " success.")
            getOutput(retval)
            updatecatalog = 1
        else:
            print("", n.url, "]:", sqlfile, " failed.")
    return updatecatalog

def getOutput(output):
    for d in output:
        for e in d:
            print(e, "|", end="")
        print()

class NodeThread(threading.Thread):
    def __init__(self, node, cmd, sqlfile, moo, updatecatalog):
        threading.Thread.__init__(self)
        self.node = node
        self.cmd = cmd
        self.sqlfile = sqlfile
        self.moo = moo
        self.updatecatalog = updatecatalog
        # 0, do not update
        # 1, update
    def run(self):
        self.updatecatalog = runCommand(self.node, self.cmd, self.sqlfile, self.moo, self.updatecatalog)

class Catalog:
    'Base Class for Catalog'
    def __init__(self, hostname, username, passwd, db, num, url, port):
        self.hostname = hostname.replace(" ", "")
        self.username = username.replace(" ", "")
        self.passwd = passwd.replace(" ", "")
        self.db = db.replace(" ", "")
        self.num = num
        self.url = url
        self.port = port
    def initialize(self, hostname, username, passwd, db, num, url, port):
        self.hostname = hostname.replace(" ", "")
        self.username = username.replace(" ", "")
        self.passwd = passwd.replace(" ", "")
        self.db = db.replace(" ", "")
        self.num = num
        self.url = url
        self.port = port
    def display(self):
        print("Hostname: ", self.hostname, " Username: ", self.username, " Passwd: ", self.passwd, " DB: ", self.db)
    def update(self, table, n):
        try:
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            # checking if node's table already exists
            cmd = "select count(tname) from dtables where tname='%s' and nodeurl='%s'" % (table, n.url)
            cur.execute(cmd)
            data = cur.fetchone()[0]
            # if data > 0 then node's table already exists
            if int(data) > 0:
                # cmd = "UPDATE dtables SET tname='%s', nodedriver=NULL, nodeurl='%s', nodeuser='%s', nodepasswd='%s', partmtd='%s', nodeid='%s', partcol='%s', partparam1='%s', partparam2='%s' WHERE tname='%s' and nodeid='%s'" % (table, n.url, n.username, n.passwd, partmtd, n.num, n.partcol, n.param1, n.param2, table, n.num)
                cmd = "UPDATE dtables SET tname='%s', nodedriver=NULL, nodeurl='%s', nodeuser='%s', nodepasswd='%s', partmtd=%s, nodeid='%s', partcol='%s', partparam1='%s', partparam2='%s' WHERE tname='%s' and nodeid='%s'" % (table, n.url, n.username, n.passwd, "NULL", n.num, "NULL", "NULL", "NULL", table, n.num)
                cur.execute(cmd)
                connect.commit()
            else:
                # cmd = "INSERT INTO dtables VALUES ('%s', NULL, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (g, n.url, n.username, n.passwd, partmtd, n.num, n.partcol, n.param1, n.param2)
                cmd = "INSERT INTO dtables VALUES ('%s', NULL, '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s')" % (table, n.url, n.username, n.passwd, "NULL", n.num, "NULL", "NULL", "NULL")
                cur.execute(cmd)
                connect.commit()
            connect.close()
        except pymysql.Error:
            return 0
        return 1
    def update_pt(self, table, n, mtd, mtdinfo):
        updated = 1
        if mtd == 0:
            sql = "UPDATE dtables SET partmtd=%s WHERE (tname='%s')" % (mtd, mtdinfo)
        if mtd == 1:
            sql = "UPDATE dtables SET partmtd='%s', partcol='%s', partparam1='%s', partparam2='%s' WHERE (tname='%s' && nodeid='%s')" % (mtd, mtdinfo[4], mtdinfo[5], mtdinfo[6], mtdinfo[0], mtdinfo[3])
        elif mtd == 2:
            nodeid = "NULL"
            partcol = methodinfo[1]
            partp1 = methodinfo[2]
            partp2 = "NULL"
            sql = "UPDATE dtables SET partmtd='%s', partcol='%s', partparam1='%s', partparam2=%s WHERE tname='%s'" % (mtd, partcol, partp1, partp2, mtdinfo[0])
        try:
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            cur.execute(sql)
            connect.commit()
            connect.close()
        except pymysql.InternalError:
            updated = 0
        except pymysql.OperationalError:
            updated = 0
        return updated
    def countNodes(self, tablename):
        data = 0
        cmd = "select count(tname) from dtables where tname='%s'" % (tablename)
        try:
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            cur.execute(cmd)
            data = cur.fetchone()[0]
            connect.close()
        except pymysql.OperationalError:
            print("[", catalog.url, "]:", ddlfile, " failed.")
        return data
    def create(self):
        try:
            cmd = "CREATE TABLE dtables (tname VARCHAR(32), nodedriver VARCHAR(64), nodeurl VARCHAR(128), nodeuser VARCHAR(16), nodepasswd VARCHAR(16), partmtd INT, nodeid INT, partcol VARCHAR(32), partparam1 VARCHAR(32), partparam2 VARCHAR(32))"
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            cur.execute(cmd)
            connect.close()
        except pymysql.InternalError:
            pass
    def read(self):
        nodeurls = []
        nodes = []
        try:
            cmd = "select * from dtables"
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
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
                t = Node(hostname, nodeuser, nodepasswd, db, nodeid, nodeurl, port)
                if nodeurl not in nodeurls:
                    nodeurls.append(nodeurl)
                    nodes.append(t)
            connect.close()
        except pymysql.OperationalError:
            print("[", self.url, "]:", sqlfile, " failed.")
        return nodes
    def insert0(self, header, nodes, csvcontents, tname):
        for n in nodes:
            count = 0
            for c in csvcontents:
                count += n.update(', '.join("'{0}'".format(w.strip()) for w in c), tname)
            print("[", n.url, "]:", count, " rows inserted.")
        if count > 0:
            self.update_pt(tname, n, 0, tname)
            print("[", self.url, "]: catalog updated.")
    def insert1(self, header, nodes, csvcontents, m, tname):
        print(m)
        # m = {table_name, partmtd, number of nodes, desired node num, col, p1, p2}
        colindex = 0
        for h in header:
            if h == m[4]:
                break
            else:
                colindex = colindex + 1
        # select nodes from csvfile (stored in csvcontents) that match range partition (m)
        # 0-based colindex

        for n in nodes:
            count = 0
            if str(n.getTables()).find(tname) > -1:
                if(int(m[3]) == n.num):
                    for c in csvcontents:
                        if int(m[5]) < int(c[colindex]):
                            if int(c[colindex]) <= int(m[6]):
                                count += n.update(', '.join("'{0}'".format(w.strip()) for w in c), str(tname))
            print("[", n.url, "]:", count, " rows inserted.")
            if count > 0:
                self.update_pt(tname, n, 1, m)
                print("[", self.url, "]: catalog updated.")
    def insert2(self, header, nodes, csvcontents, m, tname):
        # m = {table_name, partmtd, col, p1}
        colindex = 0
        for h in header:
            if h == m[2]:
                break
            else:
                colindex = colindex + 1
        # select nodes from csvfile (stored in csvcontents) that match range partition (m)
        for n in nodes:
            count = 0
            for c in csvcontents:
                if int(c[colindex]) == (colindex % int(m[3])):
                    if str(n.tname) == str(tname):
                        count += n.update(', '.join("'{0}'".format(w.strip()) for w in c))
            print("[", n.url, "]:", count, " rows inserted.")
            if count > 0:
                self.update_pt(tname, n, 2, m)
                print("[", self.url, "]: catalog updated.")
    def getuniqueurl(self, localnodes):
        temp = []
        nodeurls = []
        for n in localnodes:
            if n.url not in nodeurls:
                nodeurls.append(n.url)
                temp.append(n)
        return temp

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
    def update(self, values, table):
        # add information from csv
        try:
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            sql = "INSERT INTO %s VALUES (%s)" % (table, values)
            cur.execute(sql)
            connect.commit()
            connect.close()
            return 1
        except pymysql.InternalError:
            return 0
        except pymysql.OperationalError:
            return 0

runSQL(argv)
