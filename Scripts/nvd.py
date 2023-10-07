import json
import requests
import csv
import os

#NVD API KEY
API_KEY="e560001c-817b-4776-91b3-db77edaa157b"

#Data Collection
url = 'https://services.nvd.nist.gov/rest/json/cves/2.0'
headers = {'Authorization': 'Bearer {}'.format(API_KEY)}

offset = 0
limit = 2000
results = {}

payload = {
    'pubStartDate': '2017-08-04T00:00:00.000',
    'pubEndDate': '2017-10-22T00:00:00.000',
    'resultsPerPage': str(limit),
    'startIndex': str(offset)
}
response = requests.get(url, headers=headers, params=payload)
if response :
    data = json.loads(response.content)
    vulnerabilities = data.get('vulnerabilities', [])
    for vulnerability in vulnerabilities:
        vuln_id = vulnerability['cve']['id']
        cvss_metric_v31 = vulnerability['cve'].get('metrics', {}).get('cvssMetricV31', [{}])[0]
        cvss_data = cvss_metric_v31.get('cvssData', {})
        vuln_info = {
            'id': vuln_id,
            'descriptions': vulnerability['cve']['descriptions'],
            'attackVector': cvss_data.get('attackVector', ''),
            'vectorString': cvss_data.get('vectorString', ''),
            'attackComplexity': cvss_data.get('attackComplexity', ''),
            'confidentialityImpact': cvss_data.get('confidentialityImpact', ''),
            'integrityImpact': cvss_data.get('integrityImpact', ''),
            'availabilityImpact': cvss_data.get('availabilityImpact', ''),
            'baseScore': cvss_data.get('baseScore', ''),
            'baseSeverity': cvss_data.get('baseSeverity', ''),
            'exploitabilityScore': cvss_metric_v31.get('exploitabilityScore', ''),
            'impactScore': cvss_metric_v31.get('impactScore', '')
        }
        results[vuln_id] = vuln_info   

else:
    print(f"Request failed with status code: {response.status_code}")

print(f"results {len(results)}")

if results:
    output_directory = 'C:\Datasets'
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Specify the CSV file path
    csv_file = os.path.join(output_directory, 'vulnerabilities.csv')

    # Check if the CSV file already exists
    file_exists = os.path.exists(csv_file)

    # Open the CSV file in 'a' (append) mode if it exists, else in 'w' (write) mode
    with open(csv_file, 'a', newline='') as csvfile:
        fieldnames = results[next(iter(results))].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # If the file doesn't exist, write the header
        if not file_exists:
            writer.writeheader()

        # Write the results
        for result in results.values():
            writer.writerow(result)

    
    print(f"Fetched and exported {len(results)} records to {csv_file}")

else:
    print("No records found.")