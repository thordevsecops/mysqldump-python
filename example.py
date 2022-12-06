###########################################################
#
# Python script to invoke mysql database backup in RDS
# using mysqldump and tar utility.
#
# Written by : Rodrigo Souza Rachaus
# Github: https://github.com/thordevsecops
# Python Version: 3.10.6 not tested in another version
# Create date: Nov 30, 2022
#
##########################################################

# Import required pip library to working correctly
from asyncio import wait
import configparser
import os
import time
import datetime
import pipes
import getpass
import boto.rds
import mysql.connector
import pandas as pd
import pathlib

# Necessary parameters to read .env file
from dotenv import load_dotenv

load_dotenv()

# MySQL connection using enviroment vars
HOST=os.getenv('DB_HOST')
PORT=os.getenv('DB_PORT')
DB_USER=os.getenv('DB_USER')
DB_PASS=os.getenv('DB_PASSWORD')
BACKUP_PATH =os.getenv('DB_PATH')

# Getting current Datetime to create and segment in separate files backup in folder like string "20221130-database.gz"
DATETIME = time.strftime('%Y%m%d-%H%M%S')

import mysql.connector
conn = mysql.connector.connect (user=DB_USER, password=DB_PASS,
                               host=HOST,buffered=True)
cursor = conn.cursor()
query = ("show databases")
cursor.execute(query)

for (databases) in cursor:
     print (databases[0])

try:
    arquivo = open('./arquivo.txt', 'r+')
except FileNotFoundError:
    arquivo = open('arquivo.txt', 'w+')
    arquivo.writelines([(str(i)+'\n') for i in databases])
arquivo.close()


# Starting dumping schema
databases=('agoracia',)

def get_dump(database):
    filestamp = time.strftime('%Y-%m-%d-%I')
    os.popen("mysqldump --single-transaction --set-gtid-purged=OFF --column-statistics=0 --default-character-set=utf8 --routines -h %s -P %s -u %s -p%s %s > %s.sql" %
     (HOST,PORT,DB_USER,DB_PASS,database,database+"_"+filestamp))
    
    print("\n|| Database dumped to "+database+"_"+filestamp+".sql || ")


if __name__=="__main__":
    for database in databases:
        get_dump(database)