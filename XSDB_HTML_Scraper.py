import os
import time
import argparse
import json
from tqdm import tqdm
from bs4 import BeautifulSoup
from getpass import getpass
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

current_time = datetime.now().strftime("%Y-%m-%d_%H-%M")

def get_chrome_options():
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument("--disable-usb")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--log-level=3")  # Suppress logging of severity level 3 (INFO, WARNING, ERROR)
    # CHANGE binary_location to YOUR_PATH
    chrome_options.binary_location = "/home/zflowers/chrome/opt/google/chrome/google-chrome"
    return chrome_options

def CERN_login(driver):
    # Locate the username, password fields and the login button
    username_field = driver.find_element(By.NAME, 'username')  # Adjust the name attribute
    password_field = driver.find_element(By.NAME, 'password')  # Adjust the name attribute
    login_button = driver.find_element(By.NAME, 'login')  # Adjust the button element
    
    # Enter the credentials and click the login button
    username = getpass("Please enter your CERN username and press Enter to continue...")
    password = getpass("Please enter your CERN password and press Enter to continue...")
    username_field.send_keys(username)
    password_field.send_keys(password)
    login_button.click()
    
    # Wait for the 2FA input page to load
    try:
        two_fa_input = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "kc-otp-login-form")))
    except TimeoutException:
        print("Login failed!")
        return None
    
    # Prompt the user to manually enter the 2FA code
    two_fa_code = getpass("Please enter your CERN 2FA code and press Enter to continue...")
    
    # Find the 2FA input field and enter the code
    driver.find_element(By.ID, "otp").send_keys(two_fa_code)
    
    # Find the "Sign In" button and click it
    sign_in_button = driver.find_element(By.ID, "kc-login")
    sign_in_button.click()


def get_XSDB_Info(dataset_name="", search_field=None, driver=None, repeat=0, max_repeat=3):
    if max_repeat < 0: max_repeat = 0
    # Type dataset_name into search_field
    dataset_name = dataset_name.replace('\n', '').replace('\r', '').strip()
    search_string = "process_name="+dataset_name
    search_field.clear()
    search_field.send_keys(search_string)
    search_field.send_keys(Keys.RETURN)
    
    # Wait for page to load after searching for dataset
    time.sleep(0.25+repeat/max(1,max_repeat))
    
    # Scrape the page for info
    page_source = driver.page_source
    # Use BeautifulSoup to parse the page
    soup = BeautifulSoup(page_source, 'html.parser')
    
    tbodies = soup.find_all("tbody")
    selected_tbody = None
    for tbody in tbodies:
        if dataset_name in str(tbody.text):
            selected_tbody = tbody
            break
    
    dataset_info = []
    if selected_tbody:
        rows = [row for row in selected_tbody.find_all("tr", recursive=False)]  # Avoid nested elements
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 19:
                dataset_info.append({
                    'process_name': cells[0].get_text(strip=True),
                    'cross_section': cells[3].get_text(strip=True),
                    'total_uncertainty': cells[4].get_text(strip=True),
                    'other_uncertainty': cells[5].get_text(strip=True),
                    'accuracy': cells[6].get_text(strip=True),
                    'DAS': cells[8].get_text(strip=True),
                    'MCM': cells[9].get_text(strip=True),
                    'kFactor': cells[14].get_text(strip=True),
                    'energy': cells[17].get_text(strip=True)
                })
    elif repeat < max_repeat:
        # Repeat in case website was too slow to load
        repeat += 1
        get_XSDB_Info(dataset_name, search_field, driver, repeat)
    else:
        with open(f"failed_XSDB_datasets_{current_time}.txt", 'a') as f:
            f.write(f"{dataset_name}\n")
    return dataset_info

def user_setup():
    # Get Chrome Options
    chrome_options = get_chrome_options()
    # CHANGE ChromeDriver to YOUR_PATH
    service = Service('/ospool/cms-user/zflowers/public/chromedriver-linux64/chromedriver')
    
    # Initialize WebDriver
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Open the XSDB page
    url = 'https://xsecdb-xsdb-official.app.cern.ch/xsdb/'
    driver.get(url)
    CERN_login(driver)
    
    # Now logged in
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "searchField")))
    except TimeoutException:
        print("Login failed!")
        return None, None
    
    # Find the search field
    search_field = driver.find_element(By.ID, "searchField")

    return driver, search_field

def main(driver, search_field):
    # Loop over datasets and pull XSDB info
    parser = argparse.ArgumentParser(description="List of dataset names to process")
    parser.add_argument("--ifile", dest="dataset_list", default=None, help="Input dataset_list (.txt) containing datasets.")
    parser.add_argument("--idir", dest="dataset_list_folder", default=None, help="Input folder of dataset lists containing datasets.")
    parser.add_argument("-o", "--ofile", dest="json_output", default='info_XSDB.json', help="Output file (.json) with XSDB info.")
    parser.add_argument("-m", dest="manual_json", default='ManualRecords_XSDB.json', help="Input manual json records (for datasets known to be missing in XSDB).")
    args = parser.parse_args()
    if not args.dataset_list and not args.dataset_list_folder:
        print("Need to supply either input dataset list or folder of dataset lists!")
        return

    dataset_names = []
    # Option to skip any input files in directory
    skip_files = [] # ["102X"]

    # Read in dataset names from list
    if args.dataset_list:
        with open(args.dataset_list, "r") as f:
            dataset_names = f.readlines()

    # Read in dataset names from lists inside a folder
    elif args.dataset_list_folder:
        all_files = [os.path.join(args.dataset_list_folder, f) for f in os.listdir(args.dataset_list_folder)
                    if f.endswith(".txt") and not any (skip in f for skip in skip_files)]
        for file in all_files:
            with open(file, "r") as f:
                dataset_names += f.readlines()

    # Load in data from manually created json (for datasets known to be missing from XSDB)
    dataset_info = []
    if os.path.exists(args.manual_json):
        with open(args.manual_json, 'r') as manual_file:
            dataset_info = json.load(manual_file)
        # Extract dataset names from manual json
        manual_dataset_names = {entry["process_name"] for entry in dataset_info}
        # Remove datasets that were in manual json from list to be used with XSDB
        dataset_names = [dataset for dataset in dataset_names if dataset.replace('\n', '').replace('\r', '').strip() not in manual_dataset_names]

    # Sort list of dataset names and preserve order
    if len(dataset_names) > 1:
        dataset_names = list(dict.fromkeys(dataset_names))
    else:
        print("No dataset names in supplied input!") 
        return

    # Loop over dataset names with tqdm for progress bar
    for dataset_name in tqdm(dataset_names, desc="Getting XSDB info for datasets", unit="dataset"):
        dataset_info.extend(get_XSDB_Info(dataset_name, search_field, driver))

    # Write output to json file
    with open(args.json_output.replace('.json','')+"_"+current_time+'.json', 'w') as json_file:
        json.dump(dataset_info, json_file, indent=4)

    print("Finished getting info from XSDB!")

if __name__ == "__main__":
    driver, search_field = user_setup()
    if driver:
        print("Successfully connected to XSDB!")
        main(driver, search_field)
        # Close the browser
        driver.quit()

