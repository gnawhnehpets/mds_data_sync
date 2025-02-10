import requests
from pymongo import MongoClient
import json
import os
from dotenv import load_dotenv

load_dotenv(override = True)

def fetch_metadata(url):
    """Fetch data from the endpoint."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            # Convert the dict of objects to a list of objects
            return list(data.values())
        elif isinstance(data, list):
            return data  # Return as is if already a list
        else:
            print("Unexpected data format from API.")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return []

def save_to_mongodb(data, mongo_uri, db_name, collection_name):
    """Save data to MongoDB collection."""
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Drop the collection before inserting new data
        collection.delete_many({})
        print(f"Collection '{collection_name}' dropped.")

        # Insert data into the collection
        if isinstance(data, list):
            result = collection.insert_many(data)
            print(f"Inserted {len(result.inserted_ids)} documents into MongoDB.")
        else:
            print("Data is not a list. Skipping MongoDB insertion.")
    except Exception as e:
        print(f"Error saving data to MongoDB: {e}")
    finally:
        client.close()

def main():
    # Define parameters
    endpoint = "https://healdata.org/mds/metadata?data=True&limit=1000000"
    mongo_uri = os.getenv("MONGODB_ATLAS_SRV")
    db_name = os.getenv("MONGODB_DB_NAME")
    collection_name = os.getenv("MONGODB_DB_COLLECTION")

    # Fetch data from endpoint
    data = fetch_metadata(endpoint)
    if data:
        print("Data fetched successfully.")

        # Save data to MongoDB
        save_to_mongodb(data, mongo_uri, db_name, collection_name)
    else:
        print("No data to save.")

if __name__ == "__main__":
    main()
