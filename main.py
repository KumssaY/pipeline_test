import os
import requests
from google.cloud import bigquery
from flask import Flask

app = Flask(__name__)

@app.route('/')
def etl():
    # Load environment variables
    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('DATASET_ID')
    table_id = os.environ.get('TABLE_ID')

    # Fetch data from mock API
    response = requests.get('https://dummyjson.com/products')
    products = response.json().get('products', [])

    # Initialize BigQuery client
    client = bigquery.Client()

    # Reference to the BigQuery table
    table_ref = client.dataset(dataset_id).table(table_id)

    # Fetch existing IDs to avoid duplicates
    query = f"SELECT id FROM `{project_id}.{dataset_id}.{table_id}`"
    query_job = client.query(query)
    existing_ids = [row.id for row in query_job]

    # Filter new products
    new_products = [product for product in products if product['id'] not in existing_ids]

    # Prepare rows to insert
    rows_to_insert = [
        {
            "id": product["id"],
            "title": product["title"],
            "description": product["description"],
            "price": product["price"],
            "brand": product["brand"],
            "category": product["category"]
        }
        for product in new_products
    ]

    # Insert new rows into BigQuery
    if rows_to_insert:
        errors = client.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            return f"Encountered errors: {errors}", 500
        else:
            return f"Inserted {len(rows_to_insert)} new records.", 200
    else:
        return "No new records to insert.", 200
