import csv
import json

from pymongo import MongoClient

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="tiger"
)
# Table sequence   - Department -> Dept_loc -> Employee -> Project -> WorksOn

dbcursor = mydb.cursor()
dbcursor.execute("CREATE DATABASE IF NOT EXISTS company")

# Create Department Table--------------------------------
dbcursor.execute("DROP TABLE IF EXISTS company.department")

createdept = "CREATE TABLE IF NOT EXISTS company.department (DName VARCHAR(45) NOT NULL," \
             "DNum INT NOT NULL," \
             "ManagerSSN INT NULL," \
             "ManagerStartDt VARCHAR(45) NULL)"
dbcursor.execute(createdept)

csvfile = open('Input/DEPARTMENT.csv', 'r')
insertdept = "INSERT INTO company.department (DName, DNum, ManagerSSN, ManagerStartDt) VALUES (%s, %s, %s,%s)"

for row in csvfile:
    val = tuple(row.rstrip('\n').replace("'", '').split(','))
    dbcursor.execute(insertdept, val)

mydb.commit()
print("*********************************************** Table Dept created**************************")

# Create Employee Table--------------------------------
dbcursor.execute("DROP TABLE IF EXISTS company.employee")

createemp = "CREATE TABLE IF NOT EXISTS company.employee (`EFname` VARCHAR(45) NOT NULL," \
             " `EMiddle` VARCHAR(45) NULL," \
             "`ELname` VARCHAR(45) NULL," \
             "`ESSN` VARCHAR(45) NOT NULL," \
             "`EDOB` VARCHAR(45) NOT NULL," \
             "`EAddress` VARCHAR(45) NOT NULL," \
             "`Sex` CHAR(1) NOT NULL," \
             "`ESalary` INT NULL," \
             "`SupSSN` VARCHAR(45) NULL," \
             "`EDeptNo` INT NULL," \
             "PRIMARY KEY (`ESSN`))"

dbcursor.execute(createemp)

csvfile = open('Input/EMPLOYEE.csv', 'r')
insertemp = "INSERT INTO company.employee (EFname, EMiddle, ELname, ESSN, EDOB, EAddress, Sex, ESalary, SupSSN, EDeptNo) VALUES (%s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"

for row in csvfile:
    EFname = row.rstrip('\n').replace("'", '').split(',')[0]
    EMiddle = row.rstrip('\n').replace("'", '').split(',')[1]
    ELname = row.rstrip('\n').replace("'", '').split(',')[2]
    ESSN = row.rstrip('\n').replace("'", '').split(',')[3]
    EDOB = row.rstrip('\n').replace("'", '').split(',')[4]
    EAddress = row.rstrip('\n').replace("'", '').split(',')[5] +" "+ row.rstrip('\n').replace("'", '').split(',')[6] +" "+ row.rstrip('\n').replace("'", '').split(',')[7]
    Sex = row.rstrip('\n').replace("'", '').split(',')[8]
    ESalary = row.rstrip('\n').replace("'", '').split(',')[9]
    SupSSN = row.rstrip('\n').replace("'", '').split(',')[10]
    EDeptNo = row.rstrip('\n').replace("'", '').split(',')[11]
    dbcursor.execute(insertemp, (EFname, EMiddle, ELname, ESSN, EDOB, EAddress, Sex, ESalary, SupSSN, EDeptNo))
mydb.commit()
print("*********************************************** Table Emp created**************************")


# Create Department Table--------------------------------
dbcursor.execute("DROP TABLE IF EXISTS company.department")

createdept = "CREATE TABLE IF NOT EXISTS company.department (DName VARCHAR(45) NOT NULL," \
             "DNum INT NOT NULL," \
             "ManagerSSN INT NULL," \
             "ManagerStartDt VARCHAR(45) NULL)"
dbcursor.execute(createdept)

csvfile = open('Input/DEPARTMENT.csv', 'r')
insertdept = "INSERT INTO company.department (DName, DNum, ManagerSSN, ManagerStartDt) VALUES (%s, %s, %s,%s)"

for row in csvfile:
    val = tuple(row.rstrip('\n').replace("'", '').split(','))
    dbcursor.execute(insertdept, val)

mydb.commit()


# dbconnect = MongoClient('localhost', 27017)
#
# def get_employee() :
#    db = dbconnect.company.employee
#    db.drop()
#    csvfile = open('Input/EMPLOYEE.csv', 'r')
#    col_name = [ "eFname", "eMI", "eLname", "eSsn", "eDOB", "eAddress", "eSex", "eSalary", "eSupSsn", "eDNo"]
#    for row in csvfile:
#       db.insert_many([{col_name[0]:row.split(',')[0],
#                  col_name[1]: row.split(',')[1],
#                  col_name[2]: row.split(',')[2],
#                  col_name[3]: row.split(',')[3],
#                  col_name[4]: row.split(',')[4],
#                  col_name[5]: (row.split(',')[5]+row.split(',')[6]+row.split(',')[7]),
#                  col_name[6]: row.split(',')[8],
#                  col_name[7]: row.split(',')[9],
#                  col_name[8]: row.split(',')[10],
#                  col_name[9]: row.split(',')[11],
#                  }])
# # def print_emp() :
# #     db = dbconnect.company.employee
# #     employee_list = db.find({"eFname": "Joyce"})
#
#
#
# get_employee()
# employee_list = dbconnect.company.employee.find({"eFname": "'Joyce'"})
# for x in employee_list :
#     print(x[2])
#
# dbconnect.close()









-------------------------------------------------------
import csv
import json

from pymongo import MongoClient

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="punkwalker",
  password="Anipropank@2210"
)

dbcursor = mydb.cursor()
dbcursor.execute("CREATE DATABASE IF NOT EXISTS company")
dbcursor.execute("DROP TABLE IF EXISTS company.works_on")
dbcursor.execute("DROP TABLE IF EXISTS company.dept_loc")
dbcursor.execute("DROP TABLE IF EXISTS company.project")
dbcursor.execute("DROP TABLE IF EXISTS company.department")
dbcursor.execute("DROP TABLE IF EXISTS company.employee")

# Create Department Table--------------------------------
createdept = "CREATE TABLE IF NOT EXISTS company.department (DName VARCHAR(45) NOT NULL," \
             "DNum INT NOT NULL," \
             "ManagerSSN INT NULL," \
             "ManagerStartDt VARCHAR(45) NULL)"
dbcursor.execute(createdept)

csvfile = open('Input/DEPARTMENT.csv', 'r')
insertdept = "INSERT INTO company.department (DName, DNum, ManagerSSN, ManagerStartDt) VALUES (%s, %s, %s,%s)"

for row in csvfile:
    val = tuple(row.rstrip('\n').replace("'", '').split(','))
    dbcursor.execute(insertdept, val)

mydb.commit()

# Create Employee Table--------------------------------
createemp = "CREATE TABLE IF NOT EXISTS company.employee (`EFname` VARCHAR(45) NOT NULL," \
             " `EMiddle` VARCHAR(45) NULL," \
             "`ELname` VARCHAR(45) NULL," \
             "`ESSN` VARCHAR(45) NOT NULL," \
             "`EDOB` VARCHAR(45) NOT NULL," \
             "`EAddress` VARCHAR(45) NOT NULL," \
             "`Sex` CHAR(1) NOT NULL," \
             "`ESalary` INT NULL," \
             "`SupSSN` VARCHAR(45) NULL," \
             "`EDeptNo` INT NULL," \
             "PRIMARY KEY (`ESSN`))"

dbcursor.execute(createemp)

csvfile = open('Input/EMPLOYEE.csv', 'r')
insertemp = "INSERT INTO company.employee (EFname, EMiddle, ELname, ESSN, EDOB, EAddress, Sex, ESalary, SupSSN, EDeptNo) VALUES (%s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"

for row in csvfile:
    EFname = row.rstrip('\n').replace("'", '').split(',')[0]
    EMiddle = row.rstrip('\n').replace("'", '').split(',')[1]
    ELname = row.rstrip('\n').replace("'", '').split(',')[2]
    ESSN = row.rstrip('\n').replace("'", '').split(',')[3]
    EDOB = row.rstrip('\n').replace("'", '').split(',')[4]
    EAddress = row.rstrip('\n').replace("'", '').split(',')[5] +" "+ row.rstrip('\n').replace("'", '').split(',')[6] +" "+ row.rstrip('\n').replace("'", '').split(',')[7]
    Sex = row.rstrip('\n').replace("'", '').split(',')[8]
    ESalary = row.rstrip('\n').replace("'", '').split(',')[9]
    SupSSN = row.rstrip('\n').replace("'", '').split(',')[10]
    EDeptNo = row.rstrip('\n').replace("'", '').split(',')[11]
    dbcursor.execute(insertemp, (EFname, EMiddle, ELname, ESSN, EDOB, EAddress, Sex, ESalary, SupSSN, EDeptNo))
mydb.commit()

# Create Project Table--------------------------------

createproject = "CREATE TABLE IF NOT EXISTS company.project (" \
                " `PName` varchar(45) NOT NULL," \
                "`PNo` int NOT NULL," \
                "`PLocation` varchar(45) DEFAULT NULL," \
                "`PDeptNo` int DEFAULT NULL ," \
                "PRIMARY KEY (`PNo`))"
                # "KEY `DNum_idx` (`PDeptNo`)," \
                # "FOREIGN KEY (`PDeptNo`) REFERENCES `department` (`DNum`))"
dbcursor.execute(createproject)

csvfile = open('Input/PROJECT.csv', 'r')
insertproject = "INSERT INTO company.project (PName, PNo, PLocation, PDeptNo) VALUES (%s, %s, %s,%s)"

for row in csvfile:
    val = tuple(row.rstrip('\n').replace("'", '').split(','))
    print(val)
    dbcursor.execute(insertproject, val)

mydb.commit()

# Create Department Location Table--------------------------------

createproject = "CREATE TABLE IF NOT EXISTS company.dept_loc (" \
                " `PName` varchar(45) NOT NULL," \
                "`PNo` int NOT NULL," \
                "`PLocation` varchar(45) DEFAULT NULL," \
                "`PDeptNo` int DEFAULT NULL ," \
                "PRIMARY KEY (`PNo`))"
                # "KEY `DNum_idx` (`PDeptNo`)," \
                # "FOREIGN KEY (`PDeptNo`) REFERENCES `department` (`DNum`))"
dbcursor.execute(createproject)

csvfile = open('Input/PROJECT.csv', 'r')
insertproject = "INSERT INTO company.project (PName, PNo, PLocation, PDeptNo) VALUES (%s, %s, %s,%s)"

for row in csvfile:
    val = tuple(row.rstrip('\n').replace("'", '').split(','))
    print(val)
    dbcursor.execute(insertproject, val)

mydb.commit()


# dbconnect = MongoClient('localhost', 27017)
#
# def get_employee() :
#    db = dbconnect.company.employee
#    db.drop()
#    csvfile = open('Input/EMPLOYEE.csv', 'r')
#    col_name = [ "eFname", "eMI", "eLname", "eSsn", "eDOB", "eAddress", "eSex", "eSalary", "eSupSsn", "eDNo"]
#    for row in csvfile:
#       db.insert_many([{col_name[0]:row.split(',')[0],
#                  col_name[1]: row.split(',')[1],
#                  col_name[2]: row.split(',')[2],
#                  col_name[3]: row.split(',')[3],
#                  col_name[4]: row.split(',')[4],
#                  col_name[5]: (row.split(',')[5]+row.split(',')[6]+row.split(',')[7]),
#                  col_name[6]: row.split(',')[8],
#                  col_name[7]: row.split(',')[9],
#                  col_name[8]: row.split(',')[10],
#                  col_name[9]: row.split(',')[11],
#                  }])
# # def print_emp() :
# #     db = dbconnect.company.employee
# #     employee_list = db.find({"eFname": "Joyce"})
#
#
#
# get_employee()
# employee_list = dbconnect.company.employee.find({"eFname": "'Joyce'"})
# for x in employee_list :
#     print(x[2])
#
# dbconnect.close()