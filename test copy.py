import requests
import csv
import time
import random
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define the request URL
base_url_id = "https://register.fca.org.uk/s/sfsites/aura?r=10&other.ShPo_LEX_Reg_Search.getFirmDetails=1"
url_data = "https://register.fca.org.uk/s/sfsites/aura?r=0&other.ShPo_LEX_Reg_FirmDetail.initMethod=1&other.ShPo_LEX_Reg_Utility.GetGADetails=2&other.ShPo_LEX_Reg_Utility.GetGADetails=1&ui-self-service-components-profileMenu.ProfileMenu.getProfileMenuResponse=1"
count = 0
# Define headers for the ID request
headers_id = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://register.fca.org.uk",
    "referer": "https://register.fca.org.uk/s/search?q=mortgage&type=Companies&sortby=status",
    "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "x-b3-sampled": "0",
    "x-b3-spanid": "19d225f88158d09b",
    "x-b3-traceid": "af95cd0765afd061",
    "x-sfdc-page-scope-id": "934ced0c-8653-428a-b8d3-241bf942f968",
    "x-sfdc-request-id": "166793290000534ea5",
}

# Function to fetch account data
def fetch_account_data(id):
    headers_data = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": "https://register.fca.org.uk",
        "referer": "https://register.fca.org.uk/s/firm?id=" + str(id),
        "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "x-b3-sampled": "0",
        "x-b3-spanid": "fe1b929803411549",
        "x-b3-traceid": "a7b1ee065addcd7e",
        "x-sfdc-page-cache": "0181c2219d67fae7",
        "x-sfdc-page-scope-id": "deeb0e56-18af-402b-ae2a-f1f7fc3d5168",
        "x-sfdc-request-id": "16121000002741f1b1",
    }

    data = {
        "message": '{"actions":[{"id":"62;a","descriptor":"serviceComponent://ui.self.service.components.profileMenu.ProfileMenuController/ACTION$getProfileMenuResponse","callingDescriptor":"markup://selfService:profileMenuAPI","params":{},"version":"62.0"},{"id":"87;a","descriptor":"apex://ShPo_LEX_Reg_Utility/ACTION$GetGADetails","callingDescriptor":"markup://c:ShPo_LEX_Reg_GlobalRequirements","params":{"strProject":"NewRegister"}},{"id":"112;a","descriptor":"apex://ShPo_LEX_Utility/ACTION$GetGADetails","callingDescriptor":"markup://c:ShPo_LEX_CookieManager","params":{"strProject":"NewRegister"}},{"id":"208;a","descriptor":"apex://ShPo_LEX_Reg_Utility/ACTION$GetGADetails","callingDescriptor":"markup://c:ShPo_LEX_Reg_SearchForm","params":{"strProject":"NewRegister"},"version":null},{"id":"243;a","descriptor":"apex://ShPo_LEX_Reg_FirmDetailController/ACTION$initMethod","callingDescriptor":"markup://c:ShPo_LEX_Reg_FirmDetails","params":{"orgId":"' + str(id) + '"}}]}',
        "aura.context": '{"mode":"PROD","fwuid":"ZzhjQmRxMXdrdzhvS0RJMG5qQVdxQTdEcXI0cnRHWU0zd2xrUnFaakQxNXc5LjMyMC4y","app":"siteforce:communityApp","loaded":{"APPLICATION@markup://siteforce:communityApp":"1176_gJXcTqd3KllqEBeApbDkWQ","COMPONENT@markup://instrumentation:o11ySecondaryLoader":"335_G1NlWPtUoLRA_nLC-0oFqg"},"dn":[],"globals":{},"uad":false}',
        "aura.pageURI": "/s/firm?id=" + str(id),
        "aura.token": "null"
    }

    response = requests.post(url_data, headers=headers_data, data=data)

    # Check response status and extract data
    if response.status_code == 200:
        try:
            data = response.json()
            actions = data['actions']
            extracted_data = {}
            extracted_data['Email'] = None
            extracted_data['Phone'] = None
            extracted_data['Contact'] = None
            extracted_data['Website'] = None
            extracted_data['CompanyNo'] = None
            extracted_data['Assets'] = None
            for action in actions:
                if "returnValue" in action:
                    returnValue = action['returnValue']
                    
                    if returnValue is not None:
                        if returnValue is not None and "accnt" in returnValue:
                            accnt = returnValue['accnt']
                            if "ShGl_CompaniesHouseNumber__c" in accnt:
                                extracted_data['CompanyNo'] = accnt['ShGl_CompaniesHouseNumber__c']
                        if returnValue is not None and "ComplaintContact" in returnValue:
                            ComplaintContact = returnValue['ComplaintContact']
                            
                            if 'Email' in ComplaintContact :
                                extracted_data['Email'] = ComplaintContact['Email']
                            if 'Phone' in ComplaintContact :
                                extracted_data['Phone'] = ComplaintContact['Phone']
                            if 'Name' in ComplaintContact :
                                extracted_data['Contact'] = ComplaintContact['Name']
                            
                            
                        if returnValue is not None and "principalAddress" in returnValue:
                            principalAddress = returnValue['principalAddress']
                            extracted_data['Website'] = principalAddress['ShGl_WebsiteAddress__c'] if 'ShGl_WebsiteAddress__c' in principalAddress else None
                            if extracted_data['Email'] is None:
                                extracted_data['Email'] = principalAddress['ShGl_EmailAddress__c'] if 'ShGl_EmailAddress__c' in principalAddress else None
                            if extracted_data['Phone'] is None:
                                extracted_data['Phone'] = principalAddress['ShGl_PhoneCountryCode__c'] + principalAddress['ShGl_PhoneNumber__c'] if 'ShGl_PhoneCountryCode__c' in principalAddress and 'ShGl_PhoneNumber__c' in principalAddress else None
                        if extracted_data['Email'] is None:
                            if returnValue is not None and "ComplaintContactAddress" in returnValue:
                                ComplaintContactAddress = returnValue['ComplaintContactAddress']
                                extracted_data['Email'] = ComplaintContactAddress['ShGl_EmailAddress__c'] if 'ShGl_EmailAddress__c' in ComplaintContactAddress else None

                        if extracted_data['Phone'] is not None:
                            extracted_data['Phone'] = extracted_data['Phone'].replace("\xa0","")
                            # extracted_data['Phone'] = principalAddress['ShGl_PhoneCountryCode__c'] + principalAddress['ShGl_PhoneNumber__c'] if extracted_data['Phone'] is None else None
                            # extracted_data['Contact'] = principalAddress['ShGl_EmailAddress__c'] if extracted_data['Email'] is None else None
                        
            if extracted_data['CompanyNo'] is not None:
                extracted_data['Assets'] = getCompanyInfo(extracted_data['CompanyNo'])    
            return extracted_data
        except ValueError as e:
            print("Failed to parse JSON response:", e)
            return None
    else:
        print(f"Request failed with status code: {response.status_code}")
        return None

# Function to fetch account IDs from a specific page
def fetch_account_ids(page_no):
    data = {
        "message": '{"actions":[{"id":"4970;a","descriptor":"apex://ShPo_LEX_Reg_SearchController/ACTION$getFirmDetails","callingDescriptor":"UNKNOWN","params":{"searchValues":["mortgage"],"pageSize":"20","pageNo":' + str(page_no) + ',"typeOfSearch":"Companies","location":{"longitude":null,"latitude":null},"orderBy":"status","sectorCriteria":" includes (\'Investment\',\'Pensions\',\'Mortgage\')","hideUnauthFirm":true,"hideIntroARVal":false,"investmentTypes":[]},"storable":true}]}',
        "aura.context": '{"mode":"PROD","fwuid":"ZzhjQmRxMXdrdzhvS0RJMG5qQVdxQTdEcXI0cnRHWU0zd2xrUnFaakQxNXc5LjMyMC4y","app":"siteforce:communityApp","loaded":{"APPLICATION@markup://siteforce:communityApp":"1176_gJXcTqd3KllqEBeApbDkWQ","COMPONENT@markup://instrumentation:o11ySecondaryLoader":"335_G1NlWPtUoLRA_nLC-0oFqg"},"dn":[],"globals":{},"uad":false}',
        "aura.pageURI": "/s/search?q=mortgage&type=Companies&sortby=status",
        "aura.token": "null"
    }

    # Send the POST request
    response = requests.post(base_url_id, headers=headers_id, data=data)

    if response.status_code == 200:
        try:
            json_data = response.json()
            accs = json_data['actions'][0]['returnValue']['accDetails']
            return [acc['acc']['Id'] for acc in accs]  # Return list of accIds
        except ValueError:
            return []  # Return an empty list on error
    else:
        print(f"Failed to retrieve data for page {page_no}: {response.status_code}")
        return []  # Return an empty list on failure
def getCompanyInfo(companyNo):
    global count
    proxies = [
        
    ]

   
    count+=1
    print("count", count)
    for attempt in range(5):
    
        url = "https://open.endole.co.uk/search/?q=" + companyNo
        proxy = {
                    'http': proxies[count%len(proxies)],
                    'https': proxies[count%len(proxies)]
                }
        assets_value = None
        r = requests.get(url, proxies=proxy)
        if r.status_code == 200:
    

            soup = BeautifulSoup(r.content, 'html.parser')
            
            # Get company address
            items = soup.find_all('div', class_='result-item')
            
            for item in items:

                company_info = item.find('div', class_='_company-info grid-resp')
                
                company_no_div = company_info.find(string="Company No.").find_next('div')
                strCompanyNo = company_no_div.text.strip()
                if strCompanyNo == companyNo:
                    # print(strCompanyNo)
                    assets_div = company_info.find(string="Assets").find_next('div')
                    assets_value = assets_div.text.strip().replace('£', '')
                    break
                    # print(assets_value)
            
            return assets_value
        elif r.status_code == 429:
            wait_time = random.randint(5, 10)
            print(f"Rate limited. Waiting for {wait_time} seconds before retrying...")
            time.sleep(wait_time)
        else: 
            print(f"Failed to retrieve data for {companyNo}. Status code: {r.status_code}")
            return None
        
# Prepare to save details to a CSV file
with open('account_details.csv', mode='w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['Account ID','CompanyNo', 'Website', 'Contact', 'Email', 'Phone', 'Assets'])  # Write header

    # Using ThreadPoolExecutor for multithreading
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_account_ids, page_no): page_no for page_no in range(1, 830)}

        for future in as_completed(futures):
            page_no = futures[future]

            try:
                acc_ids = future.result()
                print(f"Fetched account IDs from page {page_no}.")
                
                # Create a second ThreadPoolExecutor for `fetch_account_data`
                with ThreadPoolExecutor(max_workers=5) as data_executor:
                    data_futures = {data_executor.submit(fetch_account_data, acc_id): acc_id for acc_id in acc_ids}

                    for data_future in as_completed(data_futures):
                        acc_id = data_futures[data_future]
                        try:
                            account_data = data_future.result()
                            if account_data:
                                
                                writer.writerow([acc_id, account_data['CompanyNo'], account_data['Website'], account_data['Contact'], account_data['Email'], account_data['Phone'], account_data['Assets']])
                        except Exception as e:
                            print(f"Error fetching account data for ID {acc_id}: {e}")
            except Exception as e:
                print(f"Error fetching account IDs from page {page_no}: {e}")
