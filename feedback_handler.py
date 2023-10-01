# Importing system dependencies and required flask modules
import sys
import flask
import mysql.connector
import datetime
import time
import json
from flask import request, jsonify
from flask import abort
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

class FeedbackHandler:
    # Function to initialize class variables
    def __init__(self,kafka_topic,database_host,database_username,database_password,database_name):
        # Initializing kafka topic and DB connection variables.
        self.kafka_topic = kafka_topic
        self.database_host = database_host        
        self.database_username = database_username
        self.database_password = database_password
        self.database_name = database_name

    # Function to initialize MySQL database connection
    def InitializeDBConnection(self):
        self.db = mysql.connector.connect(
            host=self.database_host,user=self.database_username,password=self.database_password,database=self.database_name
            )
    # Function to close MySQL database connection
    def closeDBConnection(self):
        self.db.close()

    # Function to fetch served ad details from served_ads table
    def fetchServedAdDetails(self):

        # Calling InitializeDBConnection function to initialize MySQL database connection
        self.InitializeDBConnection()

        # Initializing DB cursor
        db_cursor = self.db.cursor()

        # Query to fetch served ads details based on request_id
        sql = "select * from served_ads where request_id ='" + self.ad_request_id +"';"

        # Retrieving served ad details and assigning to the object servedAd
        db_cursor.execute(sql) 
        self.servedAd =  db_cursor.fetchall()
        
        # If ad found, returning True else returning False
        if len(self.servedAd) > 0:
            return True

        # Calling closeDBConnection function to terminate DB connection if no ad found
        self.closeDBConnection()

        return False

    # Function to derive columns "user_action" and "expenditure"
    def actionDetails(self):
        if int(self.requestData["acquisition"]) == 1:
            return ["acquisition",float(self.servedAd[0][5])]
        elif int(self.requestData["click"]) == 1:
            return ["click",float(self.servedAd[0][4])]
        else:
            return ["view",0]
    
    # Function to create Kafka Topic
    def createKafkaTopic(self, topic):
        bootstrap_servers = ['18.211.252.152:9092']
        kafka_admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        # Extracting existing topics
        server_topics = kafka_admin_client.list_topics()
        # Creating topic only when it doesn't exist already
        if self.kafka_topic not in server_topics:
            topic_list = []
            topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
            kafka_admin_client.create_topics(new_topics=topic_list, validate_only=False)
    
    # Function to create a dictionary to be sent to Kafka Queue
    def createKafkaQueue(self): 
        self.kafka_dict = {}
        self.kafka_dict["campaign_id"] = self.servedAd[0][1]
        self.kafka_dict["request_id"] = self.servedAd[0][0]  
        self.kafka_dict["user_id"] = self.servedAd[0][2]            
        self.kafka_dict["timestamp"] = str(self.servedAd[0][13])
        self.kafka_dict["user_action"] = (self.actionDetails())[0]
        self.kafka_dict["expenditure"] = (self.actionDetails())[1]         
        self.kafka_dict["auction_cpm"] = float(self.servedAd[0][3])
        self.kafka_dict["auction_cpc"] = float(self.servedAd[0][4])
        self.kafka_dict["auction_cpa"] = float(self.servedAd[0][5])
        self.kafka_dict["target_age_range"] = self.servedAd[0][6]
        # Formatting to avoid errors during csv file generation
        self.kafka_dict["target_location"] = (self.servedAd[0][7]).replace(",",";").replace("and",";").replace(" ","")
        self.kafka_dict["target_gender"] = self.servedAd[0][8]
        self.kafka_dict["target_income_bucket"] = self.servedAd[0][9]        
        self.kafka_dict["campaign_start_time"] = str(self.servedAd[0][11])
        self.kafka_dict["campaign_end_time"] = str(self.servedAd[0][12])
        

    # Function to update budget and current_slot_budget in the ads table if expenditure > 0. 
    def updateDetails(self):
        # Checking if expenditure was there for the ad campaign
        if self.kafka_dict["expenditure"] > 0:
            # Query to fetch current budget details
            sql = "select budget, current_slot_budget from ads where campaign_id = '"+self.kafka_dict["campaign_id"]+"';"

            # Initializing DB cursor
            db_cursor = self.db.cursor()

            # Retrieving budget details and assigning to the object
            db_cursor.execute(sql)
            existingBudget =  db_cursor.fetchall()

            # Checking if current budget details are available    
            if len(existingBudget) > 0:
                expenditure = float(self.kafka_dict["expenditure"])
                budget = float(existingBudget[0][0])
                current_slot_budget = float(existingBudget[0][1])

                # Calculating new budgets
                new_budget = budget - expenditure
                new_current_slot_budget = current_slot_budget - expenditure
                
                # Query to update new budgets
                update_sql = ("update ads set budget = "+ str(new_budget) +","
                            " current_slot_budget ="+ str(new_current_slot_budget) + ""
                )

                # Checking if new budget is 0, then status should be marked INACTIVE
                if new_budget <= 0:
                    update_sql += ", status ='INACTIVE'"

                update_sql += " where campaign_id = '"+self.kafka_dict["campaign_id"]+"';"

                # Executing the sql statement
                db_cursor.execute(update_sql)

                # Commiting so that changes reflect globally
                self.db.commit()

                # Calling closeDBConnection function to close DB connection
                self.closeDBConnection()

    # Function to send the data to the kafka server
    def createKafkaProducer(self):
        bootstrap_server = ['18.211.252.152:9092']
        topic_name = self.kafka_topic
        self.createKafkaTopic(topic_name)
        producer = KafkaProducer(bootstrap_servers = bootstrap_server)
        jsonData = json.dumps(self.kafka_dict)
        producer.send(topic_name, jsonData.encode('utf-8'))


    # Function to handle other function calls
    def sendDataToKafka(self):
        if self.fetchServedAdDetails():
            self.createKafkaQueue()
            self.updateDetails()
            self.createKafkaProducer()


if __name__ == "__main__":

    # Validate Command line arguments
    if len(sys.argv) != 6:
        print("Usage: <kafka_topic> <database_host>  <database_username> <database_password> <database_name>")
        exit(-1)

    # Assiging arguments with meaning full names    
    kafka_topic = sys.argv[1]
    database_host = sys.argv[2]
    database_username = sys.argv[3]
    database_password = sys.argv[4]
    database_name = sys.argv[5]

    try:
        # Initializing class variables
        feedback_handler_object = FeedbackHandler(kafka_topic, database_host,database_username,database_password,database_name)

        # Basic Flask Configuration
        app = flask.Flask(__name__)
        app.config["DEBUG"] = True # For debugging incase of errors
        app.config["RESTFUL_JSON"] = {"ensure_ascii":False} # For non-ascii characters

        # Http POST request  processing
        @app.route('/ad/<ad_request_id>/feedback', methods=['POST'])
        def request_handler(ad_request_id):

            # Assigning ad_request_id
            feedback_handler_object.ad_request_id = ad_request_id
            print(feedback_handler_object.ad_request_id)
            
            # Assigning request data
            feedback_handler_object.requestData = request.json
            # Calling handler function to call all the required functions to send data to Kafka
            feedback_handler_object.sendDataToKafka()

            return jsonify({
                    "Success": True
                    })
            

        # Hosting web service at localhost and port 8000 
        app.run(host="0.0.0.0", port=8000)

    except KeyboardInterrupt:
        print("Press Control+C again to quit!")

    finally:
        print("Process completed!")