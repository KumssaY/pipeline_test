import os
import requests
from google.cloud import bigquery
from flask import Flask

app = Flask(__name__)

@app.route('/')
def etl():
    try:
        print("Starting ETL process...")
        project_id = os.environ.get('PROJECT_ID')
        dataset_id = os.environ.get('DATASET_ID')
        table_id = os.environ.get('TABLE_ID')

        print(f"Using project: {project_id}, dataset: {dataset_id}, table: {table_id}")

        # Fetch data
        response = requests.get('https://dummyjson.com/products')
        response.raise_for_status()  # raise HTTP errors
        products = response.json().get('products', [])

        print(f"Fetched {len(products)} products from API.")

        # BigQuery
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)

        query = f"SELECT id FROM `{project_id}.{dataset_id}.{table_id}`"
        existing_ids = [row.id for row in client.query(query)]

        print(f"Found {len(existing_ids)} existing product IDs in BigQuery.")

        new_products = [p for p in products if p['id'] not in existing_ids]

        rows_to_insert = [{
            "id": p["id"],
            "title": p["title"],
            "description": p["description"],
            "price": p["price"],
            "brand": p["brand"],
            "category": p["category"]
        } for p in new_products]

        if rows_to_insert:
            errors = client.insert_rows_json(table_ref, rows_to_insert)
            if errors:
                print("Insert errors:", errors)
                return f"Errors: {errors}", 500
            print(f"Inserted {len(rows_to_insert)} new records.")
            return f"Inserted {len(rows_to_insert)} new records.", 200
        else:
            print("No new records to insert.")
            return "No new records to insert.", 200
    except Exception as e:
        print("ETL error:", e)
        return f"ETL error: {e}", 500
