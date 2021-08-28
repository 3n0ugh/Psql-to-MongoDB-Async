import asyncio
import asyncpg
import sys
#region imports
from pymongo import MongoClient
import pandas as pd
import time
from decimal import Decimal
from bson.decimal128 import Decimal128
from datetime import datetime,date, timedelta

myclient = MongoClient("mongodb://localhost:27017")
mydb = myclient["dvdrental"] # Your db_name  in mongodb
mycol = mydb["film"]  # Your collection name 
mycol.create_index("title", unique=True) # Optional to create unique index

async def insert_data(data):
    mycol.insert_many(data)

async def run():
    
    db_cl = await asyncpg.create_pool(
        host="localhost", # Your postgres host
        database="dvdrental", # Your postgres db_name
        user="postgres", # Your postgres user_name
        # password="POSTGRES_PASSWORD", # Your postgres password
        port="5432" # Your postgres port
      )

    # Timer to monitor the duration of the process
    start_time=time.time()   
    data=[]

    cnt=0
    dateCnvrt = datetime.strptime("2020-08-08 00:00:00.0","%Y-%m-%d %H:%M:%S.%f")

    async with db_cl.acquire() as con:
        async with con.transaction():

            # Your own table name instead of film and own date column instead of last_update
            count=await con.fetchval("""SELECT count(*) as cnt FROM film WHERE DATE(last_update)=$1""",dateCnvrt)
            # print(count)

            # SQL query of the columns you want to move into MongoDB
            async for cr in con.cursor("""SELECT * FROM film"""):
                cnt+=1
                
                cr=dict(cr)

                cr["film_id"]=int(cr["film_id"])
                cr["title"]=str(cr["title"])
                cr["description"]=str(cr["description"])
                cr["release_year"]=int(cr["release_year"])
                cr["language_id"]=int(cr["language_id"])
                cr["rental_duration"]=float(cr["rental_duration"])
                cr["rental_rate"]=float(cr["rental_rate"])
                cr["length"]=float(cr["length"])
                cr["replacement_cost"]=float(cr["replacement_cost"])
                cr["rating"]=str(cr["rating"])
                cr["last_update"]=datetime.strptime(str(cr["last_update"]),"%Y-%m-%d %H:%M:%S.%f")
                cr["special_features"]=str(cr["special_features"])
                cr["fulltext"]=str(cr["fulltext"])

                # print(cr)
                data.append(cr)

                # Inserts into mongodb when data is finished or every 1 million data.
                if len(data)==1000000 or count==cnt: 
                  print("inserting...")
                  await insert_data(data)
                  print("inserted.")
                  data=[]

    # stop timer
    stop=time.time()
    print(stop-start_time)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())

print("finished: ", time.time())