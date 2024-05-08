-- Databricks notebook source

-- STREAMING_TABLE_OPERATION_NOT_ALLOWED.REFRESH_DELTA_LIVE_TABLE] The operation REFRESH is not allowed

-- streaming table cannot be dropped: DROP  TABLE SQL_raw_space_events;

CREATE STREAMING TABLE SQL_raw_space_events AS
  SELECT *
    -- value::string:events,                 -- extract the field `events`
    -- to_timestamp(value::string:ts) as ts  -- extract the field `ts` and cast to timestamp
  FROM STREAM read_kafka(
    bootstrapServers => 'kafka.gcn.nasa.gov:9092',
    subscribe => 'gcn.classic.text.SWIFT_POINTDIR',

  
    -- param that seems to be supported
   `kafka.sasl.mechanism` => 'OAUTHBEARER',
    -- unsupported?
   `sasl.oauthbearer.method` =>  'oidc',
   `sasl.oauthbearer.client.id` =>  '12uku1alv4grcn2qv27o8hf587',
   `sasl.oauthbearer.client.secret` =>  '1rnftragukhprbnnte90tthn5o2r89c3pkd6ebgo28k2qct6c7ut',
   `sasl.oauthbearer.token.endpoint.url` =>  'https://auth.gcn.nasa.gov/oauth2/token'
  );

