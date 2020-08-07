import csv
import json

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