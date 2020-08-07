import csv
import json
import mysql.connector

from pymongo import MongoClient

dbconnect = MongoClient('localhost', 27017)

def get_employee() :
   db = dbconnect.company.employee
   db.drop()
   csvfile = open('Input/EMPLOYEE.csv', 'r')
   col_name = [ "eFname", "eMI", "eLname", "eSsn", "eDOB", "eAddress", "eSex", "eSalary", "eSupSsn", "eDNo"]
   for row in csvfile:
      db.insert_many([{col_name[0]:row.split(',')[0],
                 col_name[1]: row.split(',')[1],
                 col_name[2]: row.split(',')[2],
                 col_name[3]: row.split(',')[3],
                 col_name[4]: row.split(',')[4],
                 col_name[4]: row.split(',')[5],
                 col_name[4]: row.split(',')[6],
                 col_name[4]: row.split(',')[7],
                 col_name[4]: row.split(',')[8],
                 col_name[4]: row.split(',')[9],
                 }])

get_employee()
dbconnect.close()




cnx = mysql.connector.connect(user='root', password='tiger',
                              host='127.0.0.1',
                              database='company')

print("Connection created=")

mycursor = cnx.cursor()

mycursor.execute("SELECT * FROM department")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)



print("\n\n\n\Query 2==================================")
myquery = "select PName,DName from project, department where project.PDeptNo = department.DNum"
cursor1 = cnx.cursor()
cursor1.execute(myquery)
result = cursor1.fetchall()

for x in result:
  print(x)

cnx.close()




