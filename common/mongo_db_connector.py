import os
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, ConfigurationError, PyMongoError


def get_mongo_collection(connection_string, db_name, collection_name):
    if not all([connection_string, db_name, collection_name]):
        print("Error: Connection string, data name, or collection name is missing.")
        return None

    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        print("Connection to MongoDB server successful!")

        db = client[db_name]
        collection = db[collection_name]
        print(f"Successfully accessed data '{db_name}' and collection '{collection_name}'.")

        return collection

    except ConnectionFailure as e:
        print(f"MongoDB Connection Failure: Could not connect to the server. {e}")
        return None
    except ConfigurationError as e:
        print(f"MongoDB Configuration Error: Check your connection string format. {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
def read_data_from_collection(collection: Collection, query: dict = None):
    if collection is None:
        print("Error: The collection object is invalid. Cannot read data.")
        return []

    #check it should nopt be empty
    if query is None:
        query = {}

    try:
        print(f"Attempting to find documents with query: {query}")
        #convert find result into list so we can process
        documents = list(collection.find(query))

        if documents:
            print(f"Successfully found {len(documents)} document(s).")
        else:
            print("No documents matched the query.")

        return documents

    except PyMongoError as e:
        print(f"A MongoDB error occurred while reading from the collection: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred while reading data: {e}")
        return []



