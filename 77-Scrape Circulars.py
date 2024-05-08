# Databricks notebook source
import requests
from bs4 import BeautifulSoup
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema for the DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("subject", StringType(), True),
    StructField("date", StringType(), True),
    StructField("from", StringType(), True),
    StructField("via", StringType(), True),
    StructField("text", StringType(), True)
])

# Function to scrape a circular from a given URL
def scrape_circular(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    subject = soup.select_one("h1").get_text(strip=True)
    date = soup.select_one("time").get_text(strip=True)
    from_element = soup.select_one("p:contains('From')")
    from_value = from_element.get_text(strip=True).replace("From", "") if from_element else ""
    via_element = soup.select_one("p:contains('Via')")
    via_value = via_element.get_text(strip=True).replace("Via", "") if via_element else ""
    text = soup.select_one("pre").get_text(strip=True)

    return subject, date, from_value, via_value, text

# Initialize an empty list to store the circulars
circulars = []

# Scrape circulars from the specified range of URLs
for i in range(1, 36403):
    print(f"scraping circular {i}...")
    url = f"https://gcn.nasa.gov/circulars/{i}"
    try:
        subject, date, from_value, via_value, text = scrape_circular(url)
        circulars.append((i, subject, date, from_value, via_value, text))
        print("circular {circulars}")
    except:
        # Skip circulars that could not be scraped
        continue

# Create a DataFrame from the circulars
df = spark.createDataFrame(circulars, schema)

# Show the DataFrame
df.show(truncate=False)
