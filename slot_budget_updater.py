# Importing system dependencies
import sys
import mysql.connector
import datetime
from datetime import datetime as dt

class SlotBudgetUpdater:
    # Function to initialize class variables
    def __init__(self, database_host, database_username, database_password, database_name):
       
        # Initialize MySQL database connection
        self.db = mysql.connector.connect(
        host=database_host,
        user=database_username,
        password=database_password,
        database=database_name
        )

    # Function to get active ads
    def fetch_ads(self):
        # Initialize the db cursor
        db_cursor = self.db.cursor()          

        # Query to fetch active ads 
        sql_fetch_ads = 'Select * from ads where status = \'ACTIVE\' '

        # Retrieving query results and storing in activeAds
        db_cursor.execute(sql_fetch_ads)
        self.activeAds = db_cursor.fetchall()
    
    # Function to calculate current_slot_budget
    def calculate_slot_budget(self):
        slots = []
        self.current_slot_budget = 0
        # Extracting start and end timestamps
        start = dt.strptime(str(self.ad[18] + " " + self.ad[20]), "%Y-%m-%d %H:%M:%S")
        end = dt.strptime(str(self.ad[19] + " " + self.ad[21]), "%Y-%m-%d %H:%M:%S")
        # Storing current timestamp in currentTime
        currentTime = datetime.datetime.now().replace(microsecond=0)
        # Checking if ad_campaign started earlier than current time, then start is updated to current time
        if start < currentTime:
            start = currentTime
        # Calculating new slots
        while start <= end:
            slots.append(start)
            start += datetime.timedelta(minutes=10)
        # If new slots available, calculating new slot budget
        if len(slots) != 0:
            self.current_slot_budget = (float(self.ad[16])/len(slots))
        return self.current_slot_budget

    # Function to update slot budget in db
    def update_slot_budget(self):
        # Initializing the db cursor
        db_cursor = self.db.cursor()       

        # SQL query for updating current slot budget
        sql = ("UPDATE ads set current_slot_budget =%s where campaign_id =%s");
        val = (self.current_slot_budget, self.ad[3]);
        
        # Executing the sql query
        db_cursor.execute(sql, val);
        # Committing the operation to reflect results globally
        self.db.commit();

    # Function to process each ad
    def process_ad(self):
        for singleAd in self.activeAds:
            self.ad = singleAd
            self.calculate_slot_budget()
            if len(self.ad) != 0:
                self.update_slot_budget()   
                print(self.current_slot_budget, self.ad[3])       
            
    def __del__(self):
        # Cleanup database connection before termination        
        self.db.close()

if __name__ == "__main__":
    # Validate Command line arguments
    if len(sys.argv) != 5:
        print("Usage: slot_budget_updater.py <database-host> <database-user> <database-password> <database-name>")
        exit(-1)
    
    # Assigning meaningful names to arguments
    database_host = sys.argv[1]
    database_username = sys.argv[2]
    database_password = sys.argv[3]
    database_name = sys.argv[4]
    
    slot_updater = None
    try:
        # Intializing the class variables
        slot_updater = SlotBudgetUpdater(database_host, database_username, database_password, database_name)
        # Calling fetch_ads to get active ads
        slot_updater.fetch_ads()
        # Calling process_ad for current_slot_budget calculation and updating it to DB
        slot_updater.process_ad()


    except KeyboardInterrupt:
        print('KeyboardInterrupt, exiting...')
    finally:
        if slot_updater is not None:
            del slot_updater

