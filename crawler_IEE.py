import requests
from bs4 import BeautifulSoup
import time
import selenium
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



queue = []
iee_raw = open('iee_raw_html.txt','w')

def add_links(raw_html):
    return

def visit_documents(driver):
    while len(queue) > 0:
        url = queue.pop(0)
        driver.get(url)
        print("Visiting", url)
        wait = WebDriverWait(driver, 10).until(EC.any_of(EC.presence_of_element_located((By.XPATH, '//xpl-book-toc')), EC.presence_of_element_located((By.XPATH, '//xpl-document-abstract'))))
        raw_html = driver.find_element(By.XPATH, '//html').get_attribute('outerHTML')
        #print(raw_html)
        break
    pass

def get_links(driver, page_number):
    driver.get(f'https://ieeexplore.ieee.org/search/searchresult.jsp?sortType=newest&newsearch=true&pageNumber={page_number}')
    wait = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.List-results-items')))
    # wait.until(EC.presence_of_element_located((By.CLASS_NAME, "ng-SearchResults")))
    # WebDriverWait(browser, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".reply-button"))).click()


    print("Uz som pockal")
    # print(wait.get_attribute('innerHTML'))
    listRes = driver.find_elements(By.CSS_SELECTOR, '.List-results-items')

    raw_html = driver.find_element(By.XPATH, '//html').get_attribute('outerHTML')
    paths_to_ids = re.findall(r'(?:href=")\/(document|book)\/(\d+)', raw_html)
    for path in paths_to_ids:
        queue.append(f'https://ieeexplore.ieee.org/{path[0]}/{path[1]}')
    print(queue)

def crawl():
    driver = selenium.webdriver.Edge()
    page_number = 1
    while True:
        get_links(driver, page_number)
        time.sleep(1)
        visit_documents(driver)
        page_number += 1
    # print(raw_html)
    #print("This is body", wait.get_attribute('innerHTML'))
    # for item in listRes:
    #     print(item.get_attribute('innerHTML'))
    driver.quit()


    #print(response.content.decode('utf-8'))


crawl()
