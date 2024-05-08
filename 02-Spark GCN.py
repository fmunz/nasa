# Databricks notebook source

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

import certifi
from uuid import uuid4

'''
changes: 

added 'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required '
because of unable to find LoginModule class: org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule

but it still doesn't find it. on Single User cluster
on shared cluster it spins up, but then blocks in Stream Initialising 
'''



client_id="12uku1alv4grcn2qv27o8hf587"
client_secret="1rnftragukhprbnnte90tthn5o2r89c3pkd6ebgo28k2qct6c7ut"

jaas_config =   f'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
                clientId="{client_id}" \
                clientSecret="{client_secret}" ;'


#oauth.client.id
#oauth.client.secret
#ssl.protocol="SSL"

kafka_config = {
    'kafka.bootstrap.servers': 'kafka.gcn.nasa.gov:9092',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'OAUTHBEARER',
    'kafka.sasl.oauthbearer.token.endpoint.url': 'https://auth.gcn.nasa.gov/oauth2/token',
    'kafka.sasl.jaas.config': jaas_config,
    'kafka.sasl.login.callback.handler.class': 'kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler',
    #'kafka.group.id': str(uuid4()),
    'startingOffsets': 'earliest'
}


# Specify the Kafka topic to read from
topic = "gcn.classic.text.SWIFT_POINTDIR"

# Read from Kafka using spark.read.format("kafka")
df = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_config) \
    .option("subscribe", topic) \
    .load()

# Cast the "value" column to StringType
df = df.withColumn("value", col("value").cast(StringType()))

# Select the desired columns
result_df = df.select("topic", "offset", "timestamp", "value")

display(result_df)
