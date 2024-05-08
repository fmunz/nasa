-- Databricks notebook source

CREATE OR REFRESH STREAMING TABLE raw_space_events AS
  SELECT *
    -- value::string:events,                 -- extract the field `events`
    -- to_timestamp(value::string:ts) as ts  -- extract the field `ts` and cast to timestamp
   FROM STREAM read_kafka(
    bootstrapServers => 'kafka.gcn.nasa.gov:9092',
    subscribe => 'gcn.classic.text.SWIFT_POINTDIR',
    startingOffsets => 'earliest',

    -- params kafka.sasl.oauthbearer.client.id
    `kafka.sasl.mechanism` => 'OAUTHBEARER',
    `kafka.security.protocol` => 'SASL_SSL',
    `kafka.sasl.oauthbearer.token.endpoint.url` => 'https://auth.gcn.nasa.gov/oauth2/token', 
    `kafka.sasl.login.callback.handler.class` => 'kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler',


    `kafka.sasl.jaas.config` =>  
         '
          kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required
          clientId="12uku1alv4grcn2qv27o8hf587" 
          clientSecret="1rnftragukhprbnnte90tthn5o2r89c3pkd6ebgo28k2qct6c7ut" ;         
         '
  );



  



