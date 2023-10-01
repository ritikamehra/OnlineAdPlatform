# Importing required libraries
import sys
import flask
from flask import request, jsonify
import uuid
from flask import abort
import mysql.connector
import datetime

# AdServer Class with all neccessary actions to be performed within the class
class AdServer:

    def __init__(self, database_host, database_username, database_password, database_name):                
        # Initializing the MySQL database connection        
        self.db = mysql.connector.connect(
            host=database_host,user=database_username,password=database_password,database=database_name
            )

    # Function to fetch details and auction cost of winner ad
    def fetchElgibleAds(self,query_params):
        
        if "state" not in query_params or "city" not in query_params or "device_type" not in query_params:
            return abort(400)
        # Initialize DB cursor
        db_cursor = self.db.cursor()

        where_clause =[]

        # SQL query to extract auction costs of second winner
        sql_query_auctionCost = "select cpm,cpc,cpa from ( select * from ads "
        #SQL query to etxract all details of winner ad
        sql_query_auctionWin = "select *  from ( select * from ads "

        #conditions for eligible ads to have status active and current time between start and end time of campaign
        where_clause_str = 'WHERE status = \'ACTIVE\' and  CURRENT_TIMESTAMP() between TIMESTAMP(date_range_start,time_range_start) and TIMESTAMP(date_range_end,time_range_end) AND ' 

        #adding check for filter criteria
        for k,v in query_params.items():
            if k == 'device_type':
                col = 'target_device'
            elif k == 'city':
                col = 'target_city'
            elif k == 'state':
                col = 'target_state'
            where_clause.append("%s = '%s'" % (col,v))
        where_clause_str += ' AND '.join(where_clause)

        # Using order by and limit to get cost details of second winner
        sql_query_auctionCost += where_clause_str + ' order by cpm desc limit 2 ) as ads_det order by cpm limit 1;'

        # Using order by and limit to get details of winner ad
        sql_query_auctionWin += where_clause_str + " order by cpm desc limit 2) as ads_det limit 1;"

        # Assigning final details to be inserted into db
        db_cursor.execute(sql_query_auctionCost)
        self.auctionCost =  db_cursor.fetchall()

        db_cursor.execute(sql_query_auctionWin)
        self.adDetails =  db_cursor.fetchall()

    # Function to insert the details of winner with the cost details of second highest bidder into the served_ads table
    def serveAds(self, eligibleads, user_id, request_id):

        db_cursor = self.db.cursor()

        # Checking if any eligible ads
        if len(self.adDetails) != 0 and len(self.auctionCost) != 0:
            # Extracting the columns
            text = self.adDetails[0][0]
            campaign_id = self.adDetails[0][3]
            auction_cpm = self.auctionCost[0][0]
            auction_cpa = self.auctionCost[0][1]
            auction_cpc = self.auctionCost[0][2]
            target_gender = self.adDetails[0][5]
            target_income_bucket = self.adDetails[0][11]
            target_device_type = self.adDetails[0][12]
            target_age_range = str(self.adDetails[0][6]) + ' - ' + str(self.adDetails[0][7])
            target_location = self.adDetails[0][8] + ', ' + self.adDetails[0][9] + ', ' + self.adDetails[0][10]
            campaign_start_time = self.adDetails[0][18] + ' ' + self.adDetails[0][20]
            campaign_end_time = self.adDetails[0][19] + ' ' + self.adDetails[0][21]
            # Getting current time till seconds
            timestamp = datetime.datetime.now().replace(microsecond=0) 
                                
            # Inserting the details of ad served to served_ads table                                
            sql = "INSERT INTO served_ads(request_id,campaign_id,user_id,auction_cpm,auction_cpc,auction_cpa,target_age_range,target_location, target_gender,target_income_bucket,target_device_type,campaign_start_time,campaign_end_time,timestamp) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s)"
            val = (request_id, campaign_id, user_id,auction_cpm,auction_cpc,auction_cpa,target_age_range, target_location, target_gender,target_income_bucket,target_device_type,campaign_start_time,campaign_end_time, timestamp)

            db_cursor.execute(sql,val)
            # Committing to reflect results globally
            self.db.commit()        

    # Cleanup of database connection before termination
    def __del__(self):
        self.db.close()


if __name__ == "__main__":

    # Validating Command line arguments
    if len(sys.argv) != 5:
        print("Usage: <database_host> <database_username> <database_password> <database_name>")
        exit(-1)

    # Assiging arguments with meaning full names
    database_host = sys.argv[1]
    database_username = sys.argv[2]
    database_password = sys.argv[3]
    database_name = sys.argv[4]

    try:
        ad_server_object = AdServer(database_host,database_username,database_password,database_name)

        # Basic Flask Configuration
        app = flask.Flask(__name__)
        app.config["DEBUG"] = True;
        app.config["RESTFUL_JSON"] = {"ensure_ascii":False}


        # Procesing HTTP GET Request
        @app.route('/ad/user/<user_id>/serve', methods=['GET'])
        def serve(user_id):
            # Generating request identifier
            request_id = str(uuid.uuid1())

            eligibleads = ad_server_object.fetchElgibleAds(request.args)
            ad_server_object.serveAds(eligibleads,user_id,request_id)         

            # Checking if there are any elgible ads    
            if len(ad_server_object.adDetails) != 0:
                return jsonify({
                    "text": ad_server_object.adDetails[0][0],
                    "request_id": request_id
                })

            # Response for no eligble ads to serve
            return jsonify({
                "status": "No ad to serve!",
                "request_id": request_id
                })

        # Hosting web service at 0.0.0.0 and port 5000 
        app.run(host="0.0.0.0", port=5000)

    except KeyboardInterrupt:
        print("Press control+c again to quit!")

    finally:
        if ad_server_object is not None:
            del ad_server_object