import pymongo
import requests
import json
import time

# MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["bigdata"]
collection = db["events"]

# Power BI Push URL (احذف اللغة والتجربة من الرابط لأنها غير مطلوبة)
powerbi_url = "https://api.powerbi.com/beta/f2e06d3e-47a5-424e-84d5-7818cd99b0fa/datasets/aaea624a-4d3c-49e5-8fcb-ba0178c854c9/rows?experience=power-bi&key=vlgTRmSXA0Lxc6CM%2B1kuwijf8aTIlYYCZ1Ug1MxT7weJ8uS%2ByYZQ5mYxEZOJat%2B5J%2BCFLbXL7UnSgmbkWpKR8Q%3D%3D"

# Loop to push data every 10 seconds
while True:
    data = list(collection.find().sort("timestamp", -1).limit(10))
    
    rows = []
    for doc in data:
        rows.append({
            "event_id": doc.get("event_id"),
            "user_id": doc.get("user_id"),
            "event_type": doc.get("event_type"),
            "timestamp": doc.get("timestamp"),
            "device": doc.get("device")
        })

    headers = {"Content-Type": "application/json"}
    response = requests.post(powerbi_url, headers=headers, data=json.dumps(rows))

    print("Pushed", len(rows), "rows | Status:", response.status_code)
    time.sleep(10)
