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




queue = []
ieee_raw = open('data_ieee/concat/iee_raw_html.txt','a', encoding='utf-8')
ieee_visited_links = open("ieee_visited_links.txt", 'r', encoding='utf-8')
ieee_visited_pages = open("ieee_visited_pages.txt", 'a', encoding='utf-8')
info_ieee_concat = open('data_ieee/concat/ieee_raw_info.txt', 'a', encoding='utf-8')

visited = []
no_ieee_keywords = 0
no_author_keywords = 0
def extract_info(raw_html, url):
    global no_ieee_keywords
    global no_author_keywords
    title = re.search(r'<h1.+?(?:document-title).+?><span[^>]*>(.+?)<', raw_html)
    authors = re.search(r'<meta\s+name="parsely-author"\s+content="([^"]*)"', raw_html)

    content = re.search(r'class="abstract-text.*?>?.*<\/strong><div[^>]*>(.*?)<', raw_html)
    pages = re.search(r'Page\(s\): <\/strong>\s*([0-9]*)', raw_html)
    publisher = re.search(r'Publisher:\s*<\/span><!----><span .*?>(.*?)<', raw_html)
    year = re.search(r'(?:Date of Publication:|Copyright Year:)\s*.*?>\s*([^<]*)', raw_html)
    if title:
        title = title.group(1)
    if authors:
        authors = authors.group(1)
    if content:
        content = content.group(1)
    if pages:
        pages = pages.group(1)


    keywords_ieee_ul = re.search(r'IEEE\sKeywords.*?<ul[^>]*>(.*?)<\/ul>', raw_html)
    if keywords_ieee_ul:
        keywords_ieee_ul = keywords_ieee_ul.group(1)

    if keywords_ieee_ul is not None:
        keywords_ieee = re.findall(r'<li[^>]*><a[^>]*>(.*?)<\/a>', keywords_ieee_ul)
    else:
        keywords_ieee = 'Not found'
        no_ieee_keywords += 1

    author_keywords_ul = re.search(r'Author[\(s\)]*\sKeywords.*?<ul[^>]*>(.*?)<\/ul>', raw_html)
    if author_keywords_ul:
        author_keywords_ul = author_keywords_ul.group(1)

    if author_keywords_ul is not None:
        keywords_author = re.findall(r'<li[^>]*><a[^>]*>(.*?)<\/a>', author_keywords_ul)
    else:
        keywords_author = 'Not found'
        no_author_keywords += 1

    author_keys = ''
    ieee_keys = ''
    try:
        if keywords_ieee != 'Not found':
            for key_word in keywords_ieee:
                ieee_keys += key_word+';'
        if keywords_author != 'Not found':
            for iee_word in keywords_author:
                author_keys += iee_word+';'
    except:
        pass
    return f'{url}|{title}|{authors}|{content}|{publisher}|{year}|{pages}|{ieee_keys}|{author_keys}'

def get_visited_links():
    global visited
    temp = ieee_visited_links.readlines()
    for link in temp:
        visited.append(link.replace('\n',''))
    ieee_visited_links.close()

def visit_documents(driver):
    while len(queue) > 0:
        url = queue.pop(0)
        # url += "/keywords#keywords"
        if url in visited:
            continue
        driver.get(url)
        print("Visiting", url)
        time.sleep(1)
        wait = WebDriverWait(driver, 10).until(
            EC.any_of(EC.presence_of_element_located((By.XPATH, '//xpl-book-toc')), EC.presence_of_element_located((By.XPATH, '//xpl-document-abstract'))))
        try:
            # keywords_button = driver.find_element(By.XPATH, "//div[@class='accordion-item'][.//button[@id='keywords']]")
            keywords_button = driver.find_element(By.XPATH, "//button[@id='keywords']")
            print("Keywords found")
            # actions = ActionChains(driver)
            # actions.move_to_element(keywords_button).click().perform()
            keywords_button.send_keys(Keys.RETURN)
        except Exception as e:
            print("Keys not found", e)
        except KeyboardInterrupt:
            return 
        raw_html = driver.find_element(By.XPATH, '//html').get_attribute('outerHTML')
        # extract info
        info = extract_info(raw_html, url)
        doc_id = url.split('/')
        doc_id = str(doc_id[-2]+'_'+doc_id[-1])
        single_file = open('data_ieee/each_text/'+doc_id+'.txt','w', encoding='utf-8')
        single_file.write(raw_html)
        single_file.close()

        single_info_file = open('data_ieee/each_text_info/'+doc_id+'_info.txt','w', encoding='utf-8')
        single_info_file.write(info)
        single_info_file.close()

        info_ieee_concat.write(info+'\n')
        ieee_raw.write(raw_html)
        ieee_raw.write("\n<<END OF HTML>>\n")
        ieee_raw.flush()

        ieee_visited_links.write(url+'\n')
        ieee_visited_links.flush()
        visited.append(url)
        #print(raw_html)

def get_links(driver, page_number):
    driver.get(f'https://ieeexplore.ieee.org/search/searchresult.jsp?sortType=newest&newsearch=true&pageNumber={page_number}')
    wait = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.List-results-items')))


    # print(wait.get_attribute('innerHTML'))
    listRes = driver.find_elements(By.CSS_SELECTOR, '.List-results-items')

    raw_html = driver.find_element(By.XPATH, '//html').get_attribute('outerHTML')
    # r'(?:href=")\/(document|book)\/(\d+)'
    paths_to_ids = re.findall(r'(?:href=")\/(document)\/(\d+)', raw_html)
    for path in paths_to_ids:
        url = f'https://ieeexplore.ieee.org/{path[0]}/{path[1]}'
        if url not in queue and url not in visited:
            queue.append(url)
    print(queue)

def crawl():
    driver = selenium.webdriver.Edge()
    get_visited_links()
    page_number = 1
    global ieee_visited_links
    ieee_visited_links = open("ieee_visited_links.txt", 'a', encoding='utf-8')
    while True:
        try:
            get_links(driver, page_number)
            time.sleep(1)
            visit_documents(driver)
            ieee_visited_pages.write(str(page_number)+'\n')
            ieee_visited_pages.flush()
            page_number += 1
        except KeyboardInterrupt:
            ieee_raw.close()
            ieee_visited_links.close()
            ieee_visited_pages.close()
            info_ieee_concat.close()
            break
   

    # print(raw_html)
    #print("This is body", wait.get_attribute('innerHTML'))
    # for item in listRes:
    #     print(item.get_attribute('innerHTML'))
    driver.quit()




crawl()
