# Importing required libraries
import sys
import mysql.connector
import datetime
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable
import json
from datetime import datetime as dt

class KafkaMySQLSink:
    def __init__(self, kafka_bootstrap_server, kafka_topic_name, database_host,
    database_username, database_password,
    database_name):
        # Initialize Kafka Consumer
        kafka_client = KafkaClient(kafka_bootstrap_server)
        self.consumer = kafka_client.topics[kafka_topic_name].get_simple_consumer(consumer_group="campaign_id",
        auto_offset_reset=OffsetType.LATEST)

        # Initialize MySQL database connection
        self.db = mysql.connector.connect(
        host=database_host,
        user=database_username,
        password=database_password,
        database=database_name
        )

    # Function to process single row
    def process_row(self, ad_info):
        # Get the db cursor
        db_cursor = self.db.cursor()                

        # DB query for supporting UPSERT operation
        sql = ("INSERT INTO ads(text, category, keywords,campaign_id,status,target_gender,target_age_start,target_age_end, target_city,target_state,target_country,target_income_bucket,target_device,cpc,cpa,cpm,budget,current_slot_budget, date_range_start,date_range_end,time_range_start,time_range_end) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE  text =%s, category =%s, keywords =%s, status =%s, target_gender =%s,target_age_start =%s,target_age_end =%s,target_city =%s,target_state =%s,target_country =%s,target_income_bucket =%s,target_device =%s,cpc =%s,cpa =%s,cpm =%s, budget =%s,current_slot_budget =%s,date_range_start =%s, date_range_end =%s,time_range_start =%s,time_range_end =%s");
        val = (ad_info["text"],ad_info["category"],ad_info["keywords"],ad_info["campaign_id"],ad_info["status"],ad_info["target_gender"],ad_info['target_age_start'],ad_info['target_age_end'],ad_info["target_city"], ad_info["target_state"],ad_info["target_country"],ad_info["target_income_bucket"],ad_info["target_device"],ad_info["cpc"],ad_info["cpa"],ad_info["cpm"],ad_info["budget"],ad_info["current_slot_budget"], ad_info["date_range_start"],ad_info["date_range_end"],ad_info["time_range_start"],ad_info["time_range_end"],ad_info["text"],ad_info["category"],ad_info["keywords"],ad_info["status"],ad_info["target_gender"],ad_info["target_age_start"],ad_info["target_age_end"], ad_info["target_city"],ad_info["target_state"],ad_info["target_country"],ad_info["target_income_bucket"],ad_info["target_device"],ad_info["cpc"],ad_info["cpa"],ad_info["cpm"],ad_info["budget"], ad_info["current_slot_budget"],ad_info["date_range_start"],ad_info["date_range_end"],ad_info["time_range_start"],ad_info["time_range_end"]);
        
        db_cursor.execute(sql, val);
        # Committing the operation to reflect results globally
        self.db.commit();

    # Function to process kafka queue messages
    def process_events(self):
        # Function to derive columns
        def derive_cols(adInfo):
            # Deriving current_slot_budget
            slots = []
            start = dt.strptime(adInfo["date_range"]["start"]+" "+adInfo["time_range"]["start"], "%Y-%m-%d %H:%M:%S")
            end = dt.strptime(adInfo["date_range"]["end"]+" "+adInfo["time_range"]["end"], "%Y-%m-%d %H:%M:%S")
            while start <= end:
                    slots.append(start)
                    start += datetime.timedelta(minutes=10)
            adInfo["current_slot_budget"] = (float(adInfo["budget"])/len(slots))
            slots = []
            # Deriving status:
            if adInfo["action"] == "Stop Campaign":
                adInfo["status"] = "INACTIVE"
            else:
                adInfo["status"] = "ACTIVE"
            # Deriving cpm
            adInfo["cpm"] = (0.0075 * float(adInfo["cpc"])) + (0.0005 * float(adInfo["cpa"]))
            # Extracting other columns
            adInfo["target_age_start"] = adInfo['target_age_range']['start']
            adInfo["target_age_end"] = adInfo['target_age_range']['end']
            adInfo["date_range_start"] = adInfo["date_range"]["start"]
            adInfo["date_range_end"] = adInfo["date_range"]["end"]
            adInfo["time_range_start"] = adInfo["time_range"]["start"]
            adInfo["time_range_end"] = adInfo["time_range"]["end"]
            return adInfo
        try:            
            for queue_message in self.consumer:
                if queue_message is not None:
                    # Fetching message from queue
                    msg = json.loads(queue_message.value)
                    # Deriving columns for each msg
                    ad_info = derive_cols(msg)
                    sep = " | "
                    # Printing the details on console
                    print(ad_info["campaign_id"],sep,ad_info["action"],sep,ad_info["status"])
                    # Calling process row function to insert data into db
                    self.process_row(ad_info)

        # In case Kafka connection errors, we will restart consumer and start processing
        except (SocketDisconnectedError, LeaderNotAvailable) as e:
            self.consumer.stop()
            self.consumer.start()
            self.process_events()
            
    def __del__(self):
        # Cleanup consumer and database connection before termination
        self.consumer.stop()
        self.db.close()

if __name__ == "__main__":
    # Validate Command line arguments
    if len(sys.argv) != 7:
        print("Usage: ad_manager.py <kafka-broker> <kafka-topic> <database-host> <database-user> <database-password> <database-name>")
        exit(-1)
    kafka_bootstrap_server = sys.argv[1]
    kafka_topic = sys.argv[2]
    database_host = sys.argv[3]
    database_username = sys.argv[4]
    database_password = sys.argv[5]
    database_name = sys.argv[6]
    ad_manager = None
    kafka_mysql_sink = None
    try:
        kafka_mysql_sink = KafkaMySQLSink(kafka_bootstrap_server, kafka_topic, database_host, database_username, database_password, database_name)
        kafka_mysql_sink.process_events()
    except KeyboardInterrupt:
        print('KeyboardInterrupt, exiting...')
    finally:
        if kafka_mysql_sink is not None:
            del kafka_mysql_sink

