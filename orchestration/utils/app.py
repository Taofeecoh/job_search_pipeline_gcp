
import os
from dotenv import load_dotenv
from module import get_data
from google.cloud import storage


load_dotenv()
url = "https://active-jobs-db.p.rapidapi.com/active-ats-24h"


querystring = {
    "limit":"10",
    "offset":"0",
    "title_filter":"Data Engineer",
    "location_filter":"United States",
    "description_type":"text"
    }

headers = {
	"x-rapidapi-key": os.getenv("API_KEY"),
	"x-rapidapi-host": "active-jobs-db.p.rapidapi.com"
}

extracted_data = get_data(url, querystring, headers)
print(extracted_data)


storage_client = storage.Client()