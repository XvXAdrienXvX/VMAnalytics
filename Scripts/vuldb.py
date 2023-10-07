import requests
import argparse
import json

API_KEY="446b79b4e039fc3dbba9968b799fdff3"


# Define arguments for API script
parser = argparse.ArgumentParser()
parser.add_argument('--details', dest='details', default=1, type=int, help='The field details is 0 or 1 and declares if the results are basic or detailed. This difference influences the API credit consumption.')
parser.add_argument('--id', dest='id', default= 5000, help='It is very simple to request the details for a vulnerability entry. The argument demands a VulDB Id.')
parser.add_argument('--fields', dest='fields', default='vulnerability_cwe,vulnerability_cvss2_vuldb_basescore,vulnerability_cvss2_nvd_basescore', help='Comma-separated list of fields to retrieve, e.g., vulnerability_cwe,vulnerability_cvss2_vuldb_basescore,vulnerability_cvss2_nvd_basescore')
args = parser.parse_args()

# Add your personal API key here
personalApiKey = API_KEY

# Set HTTP Header
userAgent = 'VulDB API Advanced Python Demo Agent'
headers = {'X-VulDB-ApiKey': personalApiKey}

# URL VulDB endpoint
url = 'https://vuldb.com/?api'
postData = {}
# Choose the API call based on the passed arguments
# Default call is the last 5 recent entries
if args.id is not None:
	postData = {'id': int(args.id)}

if args.details is not None:
	postData['details'] = int(args.details)
	
if args.fields is not None:
    postData['fields'] = args.fields

# Get API response
response = requests.post(url,headers=headers,data=postData)

# Display result if evertything went OK
if response.status_code == 200:

	# Parse HTTP body as JSON
	responseJson = json.loads(response.content)
	
	# Output
	for i in responseJson['result']:		
		print(i['entry'])
		#print(i["entry"]["id"])
		#print(i["entry"]["title"])
