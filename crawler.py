import os
import zipfile

from selenium.common.exceptions import (NoSuchElementException,
                                        StaleElementReferenceException)
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from utils import error_msg


def parse_data(publish, file_format, names, download_path, zip_file_path, unzip_dir, driver_path):
    if not os.path.exists(zip_file_path):
        opts = ChromeOptions()
        prefs = {"download.default_directory" : download_path}
        opts.add_experimental_option("prefs",prefs)
        driver = Chrome(executable_path=driver_path, options=opts)
        root_url = 'https://plvr.land.moi.gov.tw/DownloadOpenData'
        driver.get(root_url)
        r = 0
        while True:
            try:
                if os.path.exists(zip_file_path):
                    break
                if r == 0:
                    # wait table
                    not_current_term = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.ID, 'ui-id-2')))
                    not_current_term.click()
                    
                    # wait download
                    downloadTypeId2 = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.ID, 'downloadTypeId2')))
                    downloadTypeId2.click()
                    
                    tables = driver.find_elements(By.ID, 'table5')
                    
                    # wait select
                    publish_select = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.ID, 'historySeason_id')))
                    publish_select = Select(publish_select)
                    publish_select.select_by_visible_text(publish)
                    
                    county_table = tables[2]
                    trs = county_table.find_elements(By.TAG_NAME,'tr')
                    
                    file_format_select = Select(trs[0].find_element(By.ID, 'fileFormatId'))
                    file_format_select.select_by_value(file_format)
                                
                    for tr in trs:
                        if tr.text in names:
                            # get the first one
                            name_lvr_land_A = tr.find_element(By.TAG_NAME, 'input')
                            name_lvr_land_A.click()

                    file_download_button = trs[0].find_element(By.ID, 'downloadBtnId')
                    file_download_button.click()
                    
                    r += 1
                r += 1
            except NoSuchElementException as e:
                print ("No Element Found", error_msg(e).details_message)
                break
            except StaleElementReferenceException as e: 
                print ("No Element Found", error_msg(e).details_message)
                break
        driver.close()
        driver.quit()

    if not os.path.exists(unzip_dir):
        with zipfile.ZipFile(zip_file_path, 'r') as zf:
            zf.extractall(unzip_dir)
