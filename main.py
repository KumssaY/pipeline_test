import requests
from google.cloud import bigquery
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Fetch the environment variables
api_url = os.getenv("DUMMY_PRODUCTS_API_URL")
table_id = f"{os.getenv('BIGQUERY_PROJECT')}.{os.getenv('BIGQUERY_DATASET')}.products"


def fetch_dummy_products(request):
  print("test 1")
  url = api_url
  response = requests.get(url)
  products = response.json().get('products', [])
  print("test 2")
  # Init BigQuery client
  client = bigquery.Client()
  table__id = table_id

  rows_to_insert = []

  for product in products:
      rows_to_insert.append({
          "id": product["id"],
          "title": product["title"],
          "price": product["price"],
          "updated_at": datetime.now(timezone.utc).isoformat()
      })

  # Insert rows - duplicates will fail unless you use MERGE in advanced case
  errors = client.insert_rows_json(table__id, rows_to_insert)

  if errors:
      return f"Errors occurred: {errors}", 500
  return "Inserted new products successfully! ✅"
