import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
import csv
from concurrent.futures import ThreadPoolExecutor
from openpyxl import load_workbook
import argparse
count = 0
def getCompanyInfo(companyNo, proxies, retries=5, delay=5):
    global count
    try:
        count+=1
        proxy = {
            'http': proxies[count%len(proxies)],
            'https': proxies[count%len(proxies)]
        }
        url = "https://open.endole.co.uk/search/?q=" + companyNo
        r = requests.get(url)
        # Retry on 429 status with exponential backoff
        for attempt in range(retries):
            if r.status_code == 429:
                print(f"Rate limit exceeded. Waiting {delay * (2 ** attempt)} seconds before retrying...", count)
                time.sleep(delay * (2 ** attempt))
                count+=1
                proxy = {
                    'http': proxies[count%len(proxies)],
                    'https': proxies[count%len(proxies)]
                }
                # time.sleep(delay * (2 ** attempt))  # Exponential backoff
                r = requests.get(url)
            else:
                break
        
        
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'html.parser')
        
        # Get company address
        items = soup.find_all('div', class_='result-item')
        assets_value = None
        for item in items:

            company_info = item.find('div', class_='_company-info grid-resp')
            
            company_no_div = company_info.find(string="Company No.").find_next('div')
            strCompanyNo = company_no_div.text.strip()
            if strCompanyNo == companyNo:
                
                assets_div = company_info.find(string="Assets").find_next('div')
                assets_value = assets_div.text.strip().replace('£', '')
                
        time.sleep(2)
        return assets_value
    except requests.RequestException as e:
        print(f"Request error: {e}")
        return None
    except Exception as e:
        print(f"Error parsing company info: {e}")
        return None



def main(start, end):
    proxies = [
        
    ]
    
    file_path = 'accounts.xlsx'
    output_path = 'accounts_with_assets_' + str(end)+'.xlsx'
    
    # Load Excel file with pandas
    df = pd.read_excel(file_path, engine='openpyxl')
    
    # Iterate through rows 2 to 1000 and skip rows with existing 'Assets' values
    for index, row in df.iloc[start:end].iterrows():
        company_no = row['CompanyNo']
        if pd.notna(company_no) and pd.isna(row['Assets']):  # Check if Assets is empty
            assets = getCompanyInfo(str(company_no), proxies=proxies)
            df.at[index, 'Assets'] = assets
            print(f"Updated row {index + 2} with assets: {assets}")
        else:
            print(f"Skipping row {index + 2}: CompanyNo={company_no}, Assets={row['Assets']}")
    
    # Save the updated DataFrame to a new Excel file
    df.to_excel(output_path, index=False)
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update company assets in an Excel file.")
    parser.add_argument("start_row", type=int, help="Starting row number (1-based)")
    parser.add_argument("end_row", type=int, help="Ending row number (1-based)")
    args = parser.parse_args()
    main(args.start_row, args.end_row)