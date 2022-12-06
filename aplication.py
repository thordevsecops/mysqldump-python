###########################################################
#
# Python script to invoke mysql database backup in RDS
# using mysqldump and tar utility.
#
# Written by : Thor Rachaus
# Github: https://github.com/thordevsecops
# Github Repo: https://github.com/thordevsecops/mysqldump-python
# Github Project Issues: https://github.com/thordevsecops/mysqldump-python/issues
# Linkedin: https://linkedin.com/in/thorrachaus
# Python Version: 3.10.6 not tested in version
# Initial Commit: Dec 02, 2022
# Final Commit (Production): None
# Script Revision: 0.0.1 - alpha release
#
##########################################################

# Import required pip library to working correctly
from datetime import time
import os
import time
import mysql.connector
import pipes
import pathlib

# Path directory to write all database name.
filePath = './databases.txt'

# Necessary parameters to read .env file
from dotenv import load_dotenv
load_dotenv()

# MySQL enviroment vars for working correctly
HOST = os.getenv('DB_HOST')
PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASSWORD')
BACKUP_PATH = os.getenv('DB_PATH') 
DB_NAME_FILE = os.getenv('DB_NAME_FILE') # Using this enviroment to take multiple database backup
DB_NAME = os.getenv('DB_NAME') # Using this enviroment to generate one dump

# Getting current Datetime to create and segment in separate backup in folder like string "BACKUP_PATH/DATETIME"
DATETIME = time.strftime('%Y%m%d-%H%M%S')

# Check if backup folder already exist or not. If not exist create it.


# Initialize connection to MySQL Server and obtain all database list
conn = mysql.connector.connect(user=DB_USER, password=DB_PASS,
                               host=HOST, buffered=True)
cursor = conn.cursor()
query = ("show databases")
cursor.execute(query)
for (databases) in cursor:
    print(databases[0])

# Verify 
try:
    arquivo = open('./databases.txt', 'r+')
except FileNotFoundError:
    arquivo = open('./databases.txt', 'w+')
    arquivo.writelines([(str(i) + '\n') for i in databases])
arquivo.close()

# Checking condition to backup all database list using enviroment variable (DB_NAME_FILE) or single database using (DB_NAME)
print ("checking for databases names file exist")
if os.path.exists(DB_NAME_FILE):
    file1 = open(DB_NAME_FILE)
    multi = 1
    print ("Databases file found...")
    print ("Starting backup of all dbs listed in file " + DB_NAME_FILE)
else:
    print ("Databases file not found...")
    print ("Starting backup of database " + DB_NAME)
    multi = 0

# Initialize backup MySQL database according to the condition
if multi:
    in_file = open(DB_NAME_FILE, "r")
    flength = len(in_file.readlines())
    in_file.close()
    p = 1
    db = open(DB_NAME_FILE, "r")

while p <= flength:
    dump = databasefile.readline() # Read DB_NAME_FILE line per line to execute MySQLDump
    databasefile = databasefile[:-1]  # Remove extra lines in txt file.
    dumpcmd = "mysqldump - h " + HOST + " -u " + DB_USER + " -p" + DB_PASS + " " + db + " > " + pipes.quote(TODAYBACKUPPATH) + '/' + db + ".sql"
    os.system(dumpcmd)
    gzipcmd = "gzip " + pipes.quote(TODAYBACKUPPATH) + "/" + db + ".sql"
    os.system(gzipcmd)
    p = p + 1
    databasefile.close()
else:
    db = DB_NAME
    dumpcmd = "mysqldump - h " + HOST + " -u " + DB_USER + " -p" + DB_PASS + " " + DB_NAME + " > " + pipes.quote(TODAYBACKUPPATH) + '/' + DB_NAME + ".sql"
    os.system(dumpcmd)
    gzipcmd = "gzip " + pipes.quote(TODAYBACKUPPATH) + "/" + db + ".sql"
    os.system(gzipcmd)

    print("")
    print("Application script finish")
    print("Your database backup have been created in '" + TODAYBACKUPPATH + "' directory")
