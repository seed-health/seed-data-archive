import http.client
import json
import urllib.request
import os
from datetime import datetime

conn = http.client.HTTPSConnection("backend.podscribe.ai")

headers = {
    'Accept': "application/json",
    'Authorization': "Bearer 69de8568-0eb5-4fae-950e-b1141c56ac4c"
}

# First request to get the id
conn.request("GET", "/api/public/impressions?advertiser=seed", headers=headers)

res = conn.getresponse()
data = res.read()
response_data = json.loads(data.decode("utf-8"))

# Extract the id from the response
impression_id = response_data['id']

# Second request using the extracted id
file_processor_url = f"/api/public/file-processor/status/{impression_id}"
conn.request("GET", file_processor_url, headers=headers)

res = conn.getresponse()
data = res.read()
response_data = json.loads(data.decode("utf-8"))

# Extract the final download URL
final_download_url = response_data.get('url')

# Download the CSV file
if final_download_url:
    # Create a folder to store downloaded files if not exists
    os.makedirs("downloads", exist_ok=True)
    
    # Generate a filename with a timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join("downloads", f"podscribe_performance_{timestamp}.csv")
    
    # Download the file
    urllib.request.urlretrieve(final_download_url, filename)
    
    print(f"File downloaded successfully. Saved as: {filename}")
else:
    print("Final download URL not found.")
