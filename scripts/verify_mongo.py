import sys
import os

# Add project root
sys.path.append(os.getcwd())

from dotenv import load_dotenv
load_dotenv()

from services.container import Container

def verify_mongo():
    print("Verifying MongoDB Integration...")
    
    db = Container.get_db()
    
    if db is None:
        print("ERROR: Could not get database instance. Check MONGODB_URI_LOCAL.")
        return

    print("Success: Database instance retrieved.")
    
    try:
        # Test Insert
        collection = db['test_connection']
        test_doc = {"status": "ok", "timestamp": "now"}
        result = collection.insert_one(test_doc)
        print(f"Insert Success: ID={result.inserted_id}")
        
        # Test Find
        found = collection.find_one({"_id": result.inserted_id})
        if found and found['status'] == 'ok':
            print("Find Success: Document retrieved.")
        else:
            print("Find Error: Document mismatch.")
            
        # Test Delete
        collection.delete_one({"_id": result.inserted_id})
        print("Delete Success: Document removed.")
        
    except Exception as e:
        print(f"MongoDB Operation Error: {e}")
        return

    print("MongoDB Verification Complete!")

if __name__ == "__main__":
    verify_mongo()
