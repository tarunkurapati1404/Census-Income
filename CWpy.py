import sqlite3
import os
import time
import urllib.request
import gzip
import shutil

con = ""

def init(dbName):
    print("Init con")
    global con
    con = sqlite3.connect(dbName)

def close():
    print("Closing con")
    con.close()
    print("Done")

def execQuery(sql):
    #Connect and get a cursor
    cur = con.cursor()

    #Execute
    data = cur.execute(sql)

    #Close and commit
    con.commit()

    if not data:
        return ""
    return data

def execQueryFromFile(file):
    out = []
    with open(file, "r") as f:
        data = f.readlines()
        for query in data:
            out.append(execQuery(query))
    return out

def sanityCheck(line):
    noQuotes = []
    out = ""
    for idx in range(0,len(line.strip().split(","))):
        tmp = line.split(",")[idx].strip()
        if(idx not in noQuotes) :
            tmp = "'" + tmp + "'"
        tmp += ","
        out += tmp
    return out[:-1]

def insertInto(data):
    out = []
    with open("insertInto.sql", "r") as f:
        query = f.readlines()
        execQuery(query[0] + sanityCheck(data) + ")")

def createDB(dbName):
    init(dbName)

    print("Creating DB")
    execQueryFromFile("createTables.sql")

    print("Starting data read")
    data = []
    with open("census-income.data", "r") as f:
        data = f.readlines()
    for line in data:
        insertInto(line)
    print("Done data read")

#Setup
if(not os.path.exists("census-income.data.gz")):
    print("Couldn't find file, downloading")
    urllib.request.urlretrieve("https://archive.ics.uci.edu/ml/machine-learning-databases/census-income-mld/census-income.data.gz", "census-income.data.gz")
    print("Download finished")

if(not os.path.exists("census-income.data")):
    with gzip.open('census-income.data.gz', 'rb') as f_in:
        with open('census-income.data', 'wb') as f_out:
            print("Starting extraction")
            shutil.copyfileobj(f_in, f_out)
            print("Done extracting")

if(not os.path.exists("createTables.sql")):
    with open("createTables.sql", "w") as f:
        print("Writing Create Table SQL")
        f.write("CREATE TABLE income(SS_ID INTEGER PRIMARY KEY AUTOINCREMENT,AAGE VARCHAR(100),ACLSWKR DOUBLE,ADTIND DOUBLE,ADTOCC VARCHAR(100),AHGA DOUBLE,AHRSPAY VARCHAR(100),AHSCOL VARCHAR(100),AMARITL VARCHAR(100),AMJIND VARCHAR(100),AMJOCC VARCHAR(100),ARACE VARCHAR(100),AREORGN VARCHAR(100),ASEX VARCHAR(100),AUNMEM VARCHAR(100),AUNTYPE VARCHAR(100),AWKSTAT DOUBLE,CAPGAIN DOUBLE,CAPLOSS DOUBLE,DIVVAL VARCHAR(100),FILESTAT VARCHAR(100),GRINREG VARCHAR(100),GRINST VARCHAR(100),HDFMX VARCHAR(100),HHDREL DOUBLE,MARSUPWT VARCHAR(100),MIGMTR1 VARCHAR(100),MIGMTR3 VARCHAR(100),MIGMTR4 VARCHAR(100),MIGSAME VARCHAR(100),MIGSUN DOUBLE,NOEMP VARCHAR(100),PARENT VARCHAR(100),PEFNTVTY VARCHAR(100),PEMNTVTY VARCHAR(100),PENATVTY VARCHAR(100),PRCITSHP DOUBLE,SEOTR VARCHAR(100),VETQVA DOUBLE,VETYN DOUBLE,WKSWORK DOUBLE,YEAR VARCHAR(100),TRGT VARCHAR(100));")

if(not os.path.exists("inserInto.sql")):
    with open("insertInto.sql", "w") as f:
        print("Writing Prefix SQL")
        f.write("INSERT INTO Income(AAGE, ACLSWKR, ADTIND, ADTOCC, AHGA, AHRSPAY, AHSCOL, AMARITL, AMJIND, AMJOCC, ARACE, AREORGN, ASEX, AUNMEM, AUNTYPE, AWKSTAT, CAPGAIN, CAPLOSS, DIVVAL, FILESTAT, GRINREG, GRINST, HDFMX, HHDREL, MARSUPWT, MIGMTR1, MIGMTR3, MIGMTR4, MIGSAME, MIGSUN, NOEMP, PARENT, PEFNTVTY, PEMNTVTY, PENATVTY, PRCITSHP, SEOTR, VETQVA, VETYN, WKSWORK, YEAR, TRGT) VALUES (")


print("Starting process")
#Task 1 & 2
dbFile = "DB.db"

#Only create DB if it doesn't exist
if(not os.path.exists(dbFile)):
    createDB(dbFile)
else:
    init(dbFile)
print("Database ready\n")

#Task 3
print("Calculating rage and gender data:\n")
res = execQuery("SELECT ARACE, ASEX FROM Income")
raceDict = {}
for race, sex in res:
    idStr = race + "_" + sex
    if(idStr in raceDict.keys()):
        raceDict[idStr] = raceDict[idStr] + 1
    else:
        raceDict[idStr] = 0
for key in raceDict.keys():
    print(key + " - " + str(raceDict[key]))
print("")

#Task 4
print("Calculating average Annual income")
dataDict = {}
res = execQuery("SELECT ARACE, WKSWORK, AHRSPAY FROM Income WHERE AHRSPAY > 0")
for race, weeks, pay in res:
    idStr = race
    avgPay = weeks * 40 * float(pay)
    if(idStr in dataDict.keys()):
        dataDict[idStr] = [dataDict[idStr][0] + avgPay, dataDict[idStr][1] + 1]
    else:
        dataDict[idStr] = [avgPay,1]
wageData = {}
for key in dataDict.keys():
    wageData[key] = round((dataDict[key][0] / dataDict[key][1]),2)
    print(key + " " + str(wageData[key]))

#Task 5


close()