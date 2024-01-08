"""
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Last Updated on 25th Feb, 2023
Authored by: Shashi Shekhar
Reviewed by: Reetesh varshney
"""

import mysql.connector
import asyncio
import calendar
import logging
import random
import time
import uuid
import sys

from stream_manager import (
    AssetPropertyValue,
    ExportDefinition,
    IoTSiteWiseConfig,
    MessageStreamDefinition,
    PutAssetPropertyValueEntry,
    Quality,
    ResourceNotFoundException,
    StrategyOnFull,
    StreamManagerClient,
    TimeInNanos,
    Variant
)
from stream_manager.util import Util

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# This example will create a Greengrass StreamManager stream called "SomeStream".
# The component will read from Historian database and start writing data into that stream.
# StreamManager will automatically export the written data to the property alias stored in Historian.
# This example will run forever, until the program is killed.

# The size of the StreamManager stream on disk will not exceed the default (which is 256 MB).
# Any data appended after the stream reaches the size limit, will continue to be appended, and
# StreamManager will delete the oldest data until the total stream size is back under 256MB.


def streamUnProcessedFromHistorian(client, stream_name):
    global logger

    db_host_name = "<Enter Host name or IP addres of the Historian DB>"
    db_user = "<User ID of the Historian DB>"
    db_password = "<Password of the Historian DB>"
    db_name = "<Database name>"

    # Create the connection object, one for ER_297_GENERATOR Historian table and another for ER_297_GENERATOR_UPDATE table that stoires already processed historian data
    mySQLconn = mysql.connector.connect(
        host=db_host_name, user=db_user, passwd=db_password, database=db_name)
    mySQLUpdateconn = mysql.connector.connect(
        host=db_host_name, user=db_user, passwd=db_password, database=db_name)

    try:
        quality_value = 'UNCERTAIN'
        # creating the cursor object
        cur = mySQLconn.cursor()

        # Reading the Historian data from ER_297_GENERATOR table
        cur.execute("SELECT ER.ID, ER.PROPERTY_ALIAS, ER.ASSET_VALUE, ER.DATA_QUALITY, ER.DATE_TIME  FROM ER_297_GENERATOR ER WHERE NOT EXISTS (SELECT ID FROM ER_297_GENERATOR_UPDATE AS ERU WHERE ER.ID = ERU.ID) ORDER BY ER.DATE_TIME LIMIT 1;")

        # fetching the rows from the cursor object
        result = cur.fetchall()

        for curobj in result:
            ID = curobj[0]
            property_alias_str = curobj[1]
            ASSET_VALUE = curobj[2]
            DATA_QUALITY = curobj[3]
            DATE_TIME = curobj[4]

            logger.info("ID Value retreived: %s", ID)
            logger.info("Property value retreived: %s", property_alias_str)
            logger.info("Asset value retreived: %s", ASSET_VALUE)
            logger.info("Data Quality retreived: %s", DATA_QUALITY)
            logger.info("Date-Time retreived: %s", DATE_TIME)

            # Create greengrasssdk.stream_manager.data.Quality Enum object
            if DATA_QUALITY == 'GOOD':
                quality_value = Quality.GOOD

            if DATA_QUALITY == 'BAD':
                quality_value = Quality.BAD

            if DATA_QUALITY == 'UNCERTAIN':
                quality_value = Quality.UNCERTAIN

            # Create greengrasssdk.stream_manager.data.Variant object on the Asset_Value string retreived from Historian
            variant_data = Variant(double_value=float(ASSET_VALUE))

            # Create greengrasssdk.stream_manager.data.TimeInNanos object for the current date-time
            ingestion_time_in_nanos = TimeInNanos(time_in_seconds=calendar.timegm(
                time.gmtime()) - random.randint(0, 60), offset_in_nanos=random.randint(0, 10000))

            # Create greengrasssdk.stream_manager.data.AssetPropertyValue Array object for the property values
            asset = [AssetPropertyValue(
                value=variant_data, timestamp=ingestion_time_in_nanos, quality=quality_value)]

            # Contains a list of value updates for a IoTSiteWise asset property.
            asset_property_value = PutAssetPropertyValueEntry(entry_id=str(
                ID), property_alias=property_alias_str, property_values=asset)

            # Append the asset property into Sitewise stream
            client.append_message(
                stream_name, Util.validate_and_serialize_to_json_bytes(asset_property_value))

            # Once data is read and processed from Historian table, the record in Historian_Updated table has been updated to ensure they are not read and processed again
            updated_insert_stmt = (
                "INSERT INTO ER_297_GENERATOR_UPDATE (ID, LAST_UPDATE_DATE_TIME) VALUES (%s, %s);")
            updated_data = (ID, DATE_TIME)

            try:
                logger.info("Before commit")
                updatedCur = mySQLUpdateconn.cursor()
                updatedCur.execute(updated_insert_stmt, updated_data)
                mySQLUpdateconn.commit()
                logger.info("Values committed with UID: %s", ID)
            except Exception as error:
                logger.error(error)
                logger.error(type(error))
                logger.info("Values rolled-back with UID: %s", ID)
                mySQLUpdateconn.rollback()
            finally:
                if updatedCur:
                    updatedCur.close()
                if mySQLUpdateconn:
                    mySQLUpdateconn.close()
    except Exception as errorObj:
        logger.error(errorObj)
        mySQLUpdateconn.rollback()
    finally:
        if cur:
            cur.close()
        if mySQLconn:
            mySQLconn.close()


def main(logger):
    try:
        client = StreamManagerClient()

        # Read stream_name from configuration parameter.
        stream_name = sys.argv[1]

        # If stream_name has the value as null, assign default value
        if stream_name == '':
            stream_name = "SomeStream"

        # Try deleting the stream (if it exists) so that we have a fresh start
        try:
            client.delete_message_stream(stream_name=stream_name)
        except ResourceNotFoundException:
            pass

        exports = ExportDefinition(
            iot_sitewise=[IoTSiteWiseConfig(
                identifier="IoTSiteWiseExport" + stream_name, batch_size=5)]
        )
        client.create_message_stream(
            MessageStreamDefinition(
                name=stream_name, strategy_on_full=StrategyOnFull.OverwriteOldestData, export_definition=exports
            )
        )

        logger.info(
            "Now going to start write IoTSiteWiseEntry to the stream from Historian DB")
        # Now start putting in random site wise entries.
        while True:
            logger.debug(
                "Appending new IoTSiteWiseEntry from Historian DB into stream")
            streamUnProcessedFromHistorian(client, stream_name)
            # client.append_message(stream_name, Util.validate_and_serialize_to_json_bytes(get_random_site_wise_entry()))
            time.sleep(5)
    except asyncio.TimeoutError:
        logger.error("Timed out")
    except Exception as e:
        logger.error(e)
        logger.error(type(e))


# Start up this sample code
main(logger=logging.getLogger())
