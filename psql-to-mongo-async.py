import asyncio
import asyncpg
#region imports
from pymongo import MongoClient
import pandas as pd
import time
from decimal import Decimal
from bson.decimal128 import Decimal128
from datetime import datetime,date, timedelta

myclient = MongoClient("mongodb://DB_HOST:DB_PORT")
mydb = myclient["YOUR_DB_NAME"]
mycol = mydb["YOUR_COLLECTION_NAME"]

async def insert_data(data):
    mycol.insert_many(data)

async def run():
    
    db_cl = await asyncpg.create_pool(
        host="DB_HOST",
        database="YOUR_DB_NAME",
        user="POSTGRES_USER_NAME",
        password="POSTGRES_PASSWORD",
        port="DB_PORT"
      )
    start_time=time.time()   
    data=[]

    mycol.drop() #To prevent key duplication error, because i declared the _id column myself
    cnt=0
    power = datetime.strptime("2020-08-08 00:00:00.0","%Y-%m-%d %H:%M:%S.%f")

    async with db_cl.acquire() as con:
        async with con.transaction():
          
            count=await con.fetchval("""SELECT count(*) as cnt FROM test_1 WHERE DATE(date1)=$1""",power)
            print(count)
            async for cr in con.cursor("""SELECT _id,int1,date1,varchar1 FROM test_1"""):
                cnt+=1
                
                cr=dict(cr)

                cr["_id"]=Decimal128(cr["_id"])
                cr["int1"]=int(cr["int1"])
                cr["varchar1"]=str(cr["varchar1"])
                cr["date1"]=datetime.strptime(cr["date1"],"%Y-%m-%d %H:%M:%S.%f")
                print(cr)
                data.append(cr)

                if len(data)==5 or count==cnt:
                    print("inserting...")
                    await insert_data(data)
                    print("inserted.")
                    data=[]

    stop=time.time()
    print(stop-start_time)
loop = asyncio.get_event_loop()
loop.run_until_complete(run())

print("finished: ", time.time())