import requests
from bs4 import BeautifulSoup
import time
import selenium
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains 
import csv
import os


queue = []
if not os.path.exists('visited_pages_history/'):
    os.mkdir('visited_pages_history')
if not os.path.exists('data_ieee/'):
    os.mkdir('data_ieee')
if not os.path.exists('data_ieee/concat/'):
    os.mkdir('data_ieee/concat/')

ieee_raw = open('data_ieee/concat/ieee_raw_html_new.txt','a+', encoding='utf-8')
try:
    ieee_visited_links = open("ieee_visited_links.txt", 'r', encoding='utf-8')
except:
    f = open('ieee_visited_links.txt', 'w+', encoding='utf-8')
    f.close()
    ieee_visited_links =  open("ieee_visited_links.txt", 'r', encoding='utf-8')

# info_ieee_concat = open('data_ieee/concat/ieee_raw_info.txt', 'a+', encoding='utf-8')

visited = {}

def get_visited_links():
    global visited
    temp = ieee_visited_links.readlines()
    for link in temp:
        link = link.replace('\n', '')
        visited[link] = True
    ieee_visited_links.close()

def load_content(filepath):
    file = open(filepath, 'r')
    content = file.read()
    return content

# visit documents in list
def visit_documents(driver):
    print("Documents to visit", len(queue))
    while len(queue) > 0:
        url = queue.pop(0)
        # url += "/keywords#keywords"
        if url in visited:
            continue
        driver.get(url)
        print("Visiting", url)
        time.sleep(0.3)
        wait = WebDriverWait(driver, 10).until(
            EC.any_of(EC.presence_of_element_located((By.XPATH, '//xpl-book-toc')), EC.presence_of_element_located((By.XPATH, '//xpl-document-abstract'))))
        try:
            # keywords_button = driver.find_element(By.XPATH, "//div[@class='accordion-item'][.//button[@id='keywords']]")
            keywords_button = driver.find_element(By.XPATH, "//button[@id='keywords']")
            print("Keywords found")
            keywords_button.send_keys(Keys.RETURN)
        except Exception as e:
            print("Keys not found", e)
        except KeyboardInterrupt:
            return 
        raw_html = driver.find_element(By.XPATH, '//html').get_attribute('outerHTML')
        # extract info
        #  info = extract_info(raw_html, url)
        doc_id = url.split('/')
        doc_id = str(doc_id[-2]+'_'+doc_id[-1])
        # uncomment for saving also into single files
        # single_file = open('data_ieee/each_text/'+doc_id+'.txt','w', encoding='utf-8')
        # single_file.write(raw_html)
        # single_file.close()

        raw_processed = raw_html.replace('\n', '')
        ieee_raw.write(raw_processed)
        ieee_raw.write("\n")
        ieee_raw.flush()

        ieee_visited_links.write(url+'\n')
        ieee_visited_links.flush()
        visited[url] = True
        #print(raw_html)

def get_links(driver, page_number, year,sortType):
    # https://ieeexplore.ieee.org/search/searchresult.jsp?sortType=newest&highlight=true&returnType=SEARCH&matchPubs=true&refinements=ContentType:Conferences&refinements=ContentType:Journals&returnFacets=ALL&rowsPerPage=100&pageNumber=1
    # https://ieeexplore.ieee.org/search/searchresult.jsp?sortType=newest&highlight=true&returnType=SEARCH&matchPubs=true&returnFacets=ALL&refinements=ContentType:Conferences&refinements=ContentType:Journals
    number = 0
    driver.get(f'https://ieeexplore.ieee.org/search/searchresult.jsp?sortType={sortType}&rowsPerPage=100&pageNumber={page_number}&ranges={year}_{year}_Year')
    wait = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.List-results-items')))
    # print(wait.get_attribute('innerHTML'))
    listRes = driver.find_elements(By.CSS_SELECTOR, '.List-results-items')
    raw_html = driver.find_element(By.XPATH, '//html').get_attribute('outerHTML')
    # r'(?:href=")\/(document|book)\/(\d+)'
    paths_to_ids = re.findall(r'(?:href=\")\/(document)\/(\d+)', raw_html)
    print("Extracted ", len(paths_to_ids))
    for path in paths_to_ids:
        url = f'https://ieeexplore.ieee.org/{path[0]}/{path[1]}'
        if url not in queue and url not in visited:
            queue.append(url)
            number += 1
    print(queue)

def crawl():
    contin = input("Continue on last page? y/n: ")
    start_year = int(input("start_year" ))
    end_year = int(input("end year: "))
    driver = selenium.webdriver.Edge()
    get_visited_links()
    page_number = 0
    reoccuring_documents_count = 0
    sort_Type = ['newest', 'oldest']
    print(page_number)
    global ieee_visited_links
    ieee_visited_links = open("ieee_visited_links.txt", 'a', encoding='utf-8')
    previous_number = -1
    for year in range(start_year, end_year+1):
        for sortType in sort_Type:
            try:
                ieee_visited_pages = open(f'visited_pages_history\\ieee_visited_pages{year}{sortType}.txt', 'r', encoding='utf-8')
                if contin == 'y':
                    page_number = int(ieee_visited_pages.readlines()[-1])
                    ieee_visited_pages.close()
                else:
                    page_number = 0
            except:
                page_number = 0
            ieee_visited_pages = open(f'visited_pages_history\\ieee_visited_pages_{year}{sortType}.txt', 'a+', encoding='utf-8')
            while True:
                try:
                    number = get_links(driver, page_number, year, sortType)
                    if number == 0 and previous_number == 0:
                        break
                    previous_number = number
                    visit_documents(driver)
                    ieee_visited_pages.write(str(page_number)+'\n')
                    ieee_visited_pages.flush()
                    page_number += 1
                except KeyboardInterrupt:
                    ieee_raw.close()
                    ieee_visited_links.close()
                    ieee_visited_pages.close()
                    # info_ieee_concat.close()
                    break
    driver.quit()




crawl()
