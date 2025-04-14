import os

print("Starting ETL process...")
print("ENV PROJECT_ID:", os.environ.get('PROJECT_ID'))
print("ENV DATASET_ID:", os.environ.get('DATASET_ID'))
print("ENV TABLE_ID:", os.environ.get('TABLE_ID'))