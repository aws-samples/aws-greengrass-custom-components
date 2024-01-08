"""
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Last Updated on 25th Feb, 2023
Authored by: Shashi Shekhar
Reviewed by: Reetesh varshney
"""

import mysql.connector
import logging
import random
import uuid
import time
import calendar
import datetime

logger = logging.getLogger()

# Initialize database related parameters
db_host_name = "<Enter Host name or IP addres of the Historian DB>"
db_user = "<User ID of the Historian DB>"
db_password = "<Password of the Historian DB>"
db_name = "<Database name>"

# Create the connection object
myconn = mysql.connector.connect(
    host=db_host_name, user=db_user, passwd=db_password, database=db_name)

# creating the cursor object
cur = myconn.cursor()

# Example: "/ER/297/Generator/Temperature"
property_alias = "<sitewise measurement property alias>"
temperature = 0.0000000
quality = 'UNCERTAIN'

# This method creates simulated historian record to be read by custom component


def insertIntoHistorian(uid):

    # historian data are intialized as global variables
    global temperature
    global quality
    global property_alias
    global myconn
    global cur

    ct = datetime.datetime.now()
    ts = ct.timestamp()

    # create record into ER_297_GENERATOR historian table
    insert_stmt = (
        "INSERT INTO ER_297_GENERATOR (ASSET_VALUE, PROPERTY_ALIAS, DATA_QUALITY, ID, DATE_TIME) VALUES (%s, %s, %s, %s, %s);")
    data = (temperature, property_alias, quality, uid, ct)

    try:
        cur.execute(insert_stmt, data)
        myconn.commit()
        print("Data insertion committed")
    except Exception as error:
        print(error)
        print(type(error))
        print("Data insertion rolledback")
        myconn.rollback()

# This methiod creates random measurement value to be inserted into Historian table


def getValues():
    global temperature
    global quality
    try:
        temperature = random.uniform(20, 25)  # nosec
        quality = 'GOOD'

        seed = random.uniform(1, 10)  # nosec
        logger.info("seed: {}".format(seed))
        logger.info("temperature: {}".format(temperature))

        if seed == 3 or seed == 6:
            temperature = random.uniform(35, 45)  # nosec
            quality = 'BAD'
        elif seed == 7:
            temperature = random.uniform(25, 35)  # nosec
            quality = 'UNCERTAIN'

        temperature = float(temperature)
        logger.info("temperature: {}".format(temperature))

        print(temperature)
        print(quality)

    except Exception as error:
        logger.error("error during random temperature generation: %s", error)

# To maintain a balance between number of records inserted and streamed to sitewise, data older than 8 hours will be deleted.


def deleteOlderRecord():
    global myconn
    global cur

    delete_stmt = (
        "DELETE FROM ER_297_GENERATOR WHERE DATE_TIME < now() - interval 480 MINUTE;")

    try:
        cur.execute(delete_stmt)
        myconn.commit()
        print("Data delete committed")
    except Exception as error:
        print(error)
        print(type(error))
        print("Data delete rolledback")
        myconn.rollback()


def main():
    global myconn
    while True:
        deleteOlderRecord()
        uid = str(uuid.uuid4())
        getValues()
        insertIntoHistorian(uid)
        time.sleep(2)
    myconn.close()


main()
