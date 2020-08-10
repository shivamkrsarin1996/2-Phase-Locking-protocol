import lxml.etree as ET 
from xml.etree import ElementTree
from xml.dom import minidom


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

#Reference- https://gist.github.com/Averroes/6375a1cccd39fe9f2dd7
def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = ElementTree.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


dbcursor = mydb.cursor()
dbcursor.execute("CREATE DATABASE IF NOT EXISTS company")
dbcursor.execute("use company")
dbcursor.execute("DROP TABLE IF EXISTS company.works_on")
dbcursor.execute("DROP TABLE IF EXISTS company.project")
dbcursor.execute("DROP TABLE IF EXISTS company.employee")
dbcursor.execute("DROP TABLE IF EXISTS company.dept_loc")
dbcursor.execute("DROP TABLE IF EXISTS company.department")
mydb.commit()


# Create Department Table--------------------------------
createdept = "CREATE TABLE IF NOT EXISTS company.department (DName VARCHAR(45) NOT NULL," \
             "DNum INT NOT NULL," \
             "ManagerSSN INT NULL," \
             "ManagerStartDt VARCHAR(45) NULL)"
dbcursor.execute(createdept)
mydb.commit()
print("\n*********************************************** Table Dept created**************************")

csvfile = open('Input/DEPARTMENT.csv', 'r')
insertdept = "INSERT INTO company.department (DName, DNum, ManagerSSN, ManagerStartDt) VALUES (%s, %s, %s,%s)"
for row in csvfile:
    val = tuple(row.rstrip('\n').replace("'", '').split(','))
    dbcursor.execute(insertdept, val)
mydb.commit()
# mydb.commit()
print("\n***********************************************Values inserted in Dept Table**************************")



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
mydb.commit()
print("*********************************************** Table Emp created**************************")


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
# mydb.commit()
print("\n***********************************************Values inserted in Emp Table**************************")



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
mydb.commit()
print("*********************************************** Table Project created**************************")

csvfile = open('Input/PROJECT.csv', 'r')
insertproject = "INSERT INTO company.project (PName, PNo, PLocation, PDeptNo) VALUES (%s, %s, %s,%s)"
for row in csvfile:
    val = tuple(row.rstrip('\n').replace("'", '').split(','))
    dbcursor.execute(insertproject, val)
mydb.commit()
print("\n***********************************************Values inserted in Project Table**************************")


# Create Department Location Table--------------------------------
createdept_loc = "CREATE TABLE IF NOT EXISTS company.dept_loc (" \
                "`DNum` INT NOT NULL," \
                "`DLocation` VARCHAR(45) NOT NULL)"
# "KEY `DNum_idx` (`PDeptNo`)," \
                # "FOREIGN KEY (`PDeptNo`) REFERENCES `department` (`DNum`))"
dbcursor.execute(createdept_loc)
mydb.commit()
print("\n*********************************************** Table Dept_Location created**************************")

csvfile = open('Input/DEPT_LOCATIONS.csv', 'r')
insertdept_loc = "INSERT INTO company.dept_loc (DNum, DLocation) VALUES (%s, %s)"
for row in csvfile:
    val = tuple(row.rstrip('\n').replace("'", '').split(','))
    dbcursor.execute(insertdept_loc, val)
mydb.commit()
# mydb.commit()
print("\n***********************************************Values inserted in Dept_Location Table**************************")




# Create Works_On Table--------------------------------
createworks_on = "CREATE TABLE IF NOT EXISTS company.works_on (" \
                 "`EmpSSN` CHAR(9) NOT NULL," \
                 "`PNum` INT NOT NULL," \
                 "`Hours` FLOAT NULL)"
# "KEY `DNum_idx` (`PDeptNo`)," \
                # "FOREIGN KEY (`PDeptNo`) REFERENCES `department` (`DNum`))"
dbcursor.execute(createworks_on)
mydb.commit()
print("*********************************************** Table WorksOn created**************************")

csvfile = open('Input/WORKS_ON.csv', 'r')
insertworks_on = "INSERT INTO company.works_on (EmpSSN, PNum, Hours) VALUES (%s, %s, %s)"
for row in csvfile:
    val = tuple(row.rstrip('\n').replace("'", '').split(','))
    dbcursor.execute(insertworks_on, val)
mydb.commit()
# mydb.commit()
print("\n***********************************************Values inserted in WorksOn Table**************************")



try: 
    conn = MongoClient('localhost', 27017)
    print("Connected successfully!!!") 
except:   
    print("Could not connect to MongoDB") 
  
# # database 
db = conn.company.projects
db.drop()


# Inserting Documents into Projects Collection
print("\nExecuting Query to Find Project Data\n")
DeptProjquery= "Select PName, PNo, DName from company.works_on, company.project, company.department where project.PDeptNo = department.DNum Group By PName"
dbcursor.execute(DeptProjquery)
projresult = dbcursor.fetchall()

prj_file = open("xml_file_projects.xml", 'w')
print("Xml file opened")
cmpny = ET.Element("Company")
prjcts = ET.SubElement(cmpny, "Projects") 

for x in projresult:
   # print(x)
   print("\nExecuting Query to Find Employees for"+str(x[1])+"\n")
   EmpWorksquery = "Select ELname, EFname, Hours FROM employee, works_on Where employee.ESSN = works_on.EmpSSN AND works_on.PNum = " + str(x[1])
   cur = mydb.cursor()
   cur.execute(EmpWorksquery)
   empResult = cur.fetchall()
   elist=[]

   field1 = ET.SubElement(prjcts, "ProjName") 
   field1.text = x[0] 
   field2 = ET.SubElement(prjcts, "ProjNum") 
   field2.text = str(x[1]) 
   field3 = ET.SubElement(prjcts, "DName") 
   field3.text = x[2]
   empls = ET.SubElement(prjcts, "Employees")

   for y in empResult:
        empList = ["ELname", "EFname", "Hours"]
        liststmt = {empList[0]: y[0], empList[1]: y[1],empList[2]:y[2]}
        elist.append(liststmt)


        field5 = ET.SubElement(empls, "EmpFName")
        field5.text = y[0]
        field6 = ET.SubElement(empls, "EmpLName")
        field6.text = y[1]
        field7 = ET.SubElement(empls, "Hours")
        field7.text = str(y[2])
        prettystr = prettify(cmpny)
        print(prettystr)
  
   db.insert_many([{
        "PName": x[0],
        "PNo": x[1],
        "DName": x[2],
        "Employees" : elist 
    }]) 

   
prj_file.write(prettystr)
prj_file.close()
print("=============Projects Collection Created==============")


#Inserting Documents into Employee Collection
dbemp = conn.company.employees
dbemp.drop()
print("\nExecuting Query to Find Project Data\n")
empDeptquery= "Select ELname, EFname,DName, ESSN from employee,department Where employee.EDeptNo = department.DNum"
dbcursor.execute(empDeptquery)
deptResult = dbcursor.fetchall()

emp_file = open("xml_file_employees.xml", 'w')
print("Xml file opened")
cmpny = ET.Element("Company")
empls = ET.SubElement(cmpny, "Employees")

for x in deptResult:
    # print(x)
    empProjectquery ="Select PName, PNo, Hours from employee,project,works_on Where works_on.EmpSSN =" + x[3]+" AND works_on.PNum = project.PNo group by Pname"
    cur = mydb.cursor()
    cur.execute(empProjectquery)
    empProjectResult = cur.fetchall()
    projlist = []

    field1 = ET.SubElement(empls, "EmpLName") 
    field1.text = x[0] 
    field2 = ET.SubElement(empls, "EmpFName") 
    field2.text = str(x[1]) 
    field3 = ET.SubElement(empls, "DName") 
    field3.text = x[2]
    prjcts = ET.SubElement(empls, "Projects")

    for y in empProjectResult:
        # print(y)
        pList = ["PName", "PNumber", "Hours"]
        listitm = {pList[0]: y[0], pList[1]: y[1], pList[2]: y[2]}
        projlist.append(listitm)

        field5 = ET.SubElement(prjcts, "ProjName")
        field5.text = y[0]
        field6 = ET.SubElement(prjcts, "ProjNum")
        field6.text = str(y[1])
        field7 = ET.SubElement(prjcts, "Hours")
        field7.text = str(y[2])
        prettystr = prettify(cmpny)
        print(prettystr)

    dbemp.insert_many([{
        "ELname": x[0],
        "EFname": x[1],
        "DName": x[2],
        "Projects": projlist
    }])
print("=============Employees Collection Created==============")
emp_file.write(prettystr)

emp_file.close()
print("File created") 




# # Inserting Documents into Projects Collection
# print("\nExecuting Query to Find Project Data\n")
# DeptProjquery= "Select PName, PNo, DName from company.works_on, company.project, company.department where project.PDeptNo = department.DNum Group By PName"
# dbcursor.execute(DeptProjquery)
# projresult = dbcursor.fetchall()

# for x in projresult:
#    # print(x)
#    print("\nExecuting Query to Find Employees for"+str(x[1])+"\n")
#    EmpWorksquery = "Select ELname, EFname, Hours FROM employee, works_on Where employee.ESSN = works_on.EmpSSN AND works_on.PNum = " + str(x[1])
#    cur = mydb.cursor()
#    cur.execute(EmpWorksquery)
#    empResult = cur.fetchall()
#    elist=[]

#    for y in empResult:
#         empList = ["ELname", "EFname", "Hours"]
#         liststmt = {empList[0]: y[0], empList[1]: y[1],empList[2]:y[2]}
#         elist.append(liststmt)

#    db.insert_many([{
#         "PName": x[0],
#         "PNo": x[1],
#         "DName": x[2],
#         "Employees" : elist 
#     }])    
# print("=============Projects Collection Created==============")


# root = ET.Element("company") 
# doc = ET.SubElement(root, "project") 
# field1 = ET.SubElement(doc, "ProjName") 
# field1.text = "some value1" 
# field2 = ET.SubElement(doc, "field2") 
# field2.set("name", "asdfasd") 
# field2.text = "some vlaue2" 
# prettystr = prettify(root)
# print(prettystr)
# tree = ET.ElementTree(root) 
# tree.write("filename.xml") 

# file = open("xml_file.xml", 'w')

# file.write(prettystr)
# # xml.ElementTree(root).write(file)
# file.close()

# print("File created")


# # #Inserting Department Collection
# # dbdpt = conn.company.department
# # dbdpt.drop()
# # print("\nExecuting Query to Find Department & their Manager's Data\n")
# # DeptMgrquery= "Select DName, DNum, ELname as MgrLName, EFname as MgrFName from employee, department Where department.ManagerSSN = employee.ESSN group by DName"
# # dbcursor.execute(DeptMgrquery)
# # deptMgrResult = dbcursor.fetchall()


# # for z in deptMgrResult:
# #     # print(x)
# #     #empProjectquery ="Select PName, PNo, Hours from employee,project,works_on Where works_on.EmpSSN =" + x[3]+" AND works_on.PNum = project.PNo group by Pname"
# #     empDeptquery= "Select ELname,  EFname, ESalary from employee, department Where department.DNum = employee.EDeptNo AND employee.EDeptNo = " + str(z[1])
# #     curdept = mydb.cursor()
# #     curdept.execute(empDeptquery)


# #     empDeptMgrResult = curdept.fetchall()
# #     employeelist = []
# #     for y in empDeptMgrResult:
# #         # print(y)
# #         dList = ["ELname", "EFname", "Salary"]
# #         listitms = {dList[0]: y[0], dList[1]: y[1], dList[2]: y[2]}
# #         employeelist.append(listitms)
# #     dbdpt.insert_many([{
# #         "DName": z[0],
# #         "DNumber": z[1],
# #         "ManagerLname": z[2],
# #         "ManagerFname": z[3],
# #         "Employees in Dept" : employeelist
# #     }])
# # print("=============Department Collection Created==============")

























