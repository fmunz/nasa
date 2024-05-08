# Databricks notebook source
pip install gcn-kafka astropy

# COMMAND ----------

d_catalog = "demo_frank"
d_schema = "nasa"
d_table = "raw_events"

f_schema = f"{d_catalog}.{d_schema}"
f_table = f"{f_schema}.{d_table}"

print(f"schema: {f_schema}, table: {f_table}")



# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {f_schema}")
spark.sql(f"CREATE TABLE IF NOT EXISTS {f_table}")
#spark.sql(f"USE SCHEMA  {f_schema}")


# COMMAND ----------

import pandas as pd

def msg_to_pd(msg):
    lines = msg.split('\n')
    data = {}
    comments = ''
    for line in lines:
        if line.strip():
            key_value = line.split(':')
            if key_value[0].strip() == 'COMMENTS':
                comments += key_value[1].strip() + ' '
            else:
                data[key_value[0].strip()] = key_value[1].strip()
    data['COMMENTS'] = comments.strip()
    df = pd.DataFrame(data, index=[0])
    df['all_text'] = df.astype(str).apply(', '.join, axis=1)
    return df


# COMMAND ----------

def save_df(pdf, table_name):
    df = spark.createDataFrame(pdf)
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)


# COMMAND ----------

import datetime, time
from gcn_kafka import Consumer
from pyspark.sql.types import StructType

# 'max.poll.interval.ms': 3000,
# 'session.timeout.ms': 2000  # Adjust the session timeout value



#config = {'max.poll.interval.ms': 600000, 'session.timeout.ms': 90000}
config = {}

# 'auto.offset.reset': 'earliest'


consumer = Consumer(config=config, 
                    client_id='12uku1alv4grcn2qv27o8hf587',
                    client_secret='1rnftragukhprbnnte90tthn5o2r89c3pkd6ebgo28k2qct6c7ut',
                    domain='gcn.nasa.gov')


topics = ['gcn.classic.text.SWIFT_POINTDIR']

print(f'subscribing to: {topics}')

consumer.subscribe(topics)

while True:
    
    print(f'consume({datetime.datetime.now()})', end='')
    
    for message in consumer.consume(timeout=1):
        if message.error():
            print(message.error())
            continue
        
        #regular output
        print(f'topic={message.topic()}, offset={message.offset()} \n')
        msg = message.value().decode('UTF-8')
        
        print(f'msg = {msg}')

        pdf = msg_to_pd(msg)
        save_df(pdf,f_table)
        
    print(f'- done')
    time.sleep(60)
