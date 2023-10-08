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
insect_raw = open('data_insect/concat/insect_raw_html.txt','a', encoding='utf-8')
insect_visited_links = open("insect_visited_links.txt", 'r', encoding='utf-8')

info_insect_concat = open('data_insect/concat/insect_raw_info.txt', 'a', encoding='utf-8')

visited = []
no_insect_keywords = 0
no_author_keywords = 0
def extract_info(raw_html, url):
    identifier = url.split('/')[-1]
    global no_insect_keywords
    global no_author_keywords
    name = re.search(r'<dt.*?>Name.*?<div[^>]*><span[^>]*>.*?<i>([^<]*)<\/i>([^<]*)"', raw_html)
    if name:
        name = name.group(1)+" "+name.group(2)

    published_in = re.search(r'<dt.*?>Published in.*?</dt>.*?<span[^>]*>([^<]*)', raw_html)
    status = re.search(r'<dt.*?>Checklist status.*?</dt>.*?<span[^>]*>([^<]*)', raw_html)
    synonyms = re.search(r'<dt[^>]*>Synonyms and Combinations.*?<i>([^<]*)<\/i>([^<]*)</span', raw_html)
    classification = re.search(r'<dt[^>]*>Classification</dt>.*?(unranked:.*?species.*?)</a>', raw_html)
    distributions = re.search(r'<dt.*?>Distributions.*?</dt>.*?<span>([^<]*)', raw_html)
    environemnt = re.search(r'<dt.*?>Environment\(s\).*?</dt>.*?<span[^>]*>([^<]*)', raw_html)
    references = re.search(r'<dt.*?>References</dt>.*?<\/span>.*?<span[^>]*>([^<]*)', raw_html)
    taxonimic_scrutiny = re.search(r'<dt.*?>Taxonomic scrutiny</dt>.*?<span[^>]*>([^<]*)', raw_html)
    source_dataset = re.search(r'<dt.*?>Source dataset</dt>.*?<a[^>]*>([^<]*)', raw_html)
    link_to_original = re.search(r'<dt[^>]*>Link to original resource</dt>.*?<a[^>]*>([^<]*)', raw_html)
    if classification:
        print("Found classification")
        classification = classification.group(1)
        raw_classification = re.findall(r'<[a-z][^>]*>([^>]+)<', classification)
        final_classification = 'unranked:'
        for cl in raw_classification:
            final_classification +=cl+';'
    else:
        final_classification = ''

    if link_to_original:
        link_to_original = link_to_original.group(1)
    else:
        link_to_original = ''
    if references:
        references = references.group(1)
    else:
        references = ''
    if published_in:
        published_in = published_in.group(1)
    else:
        published_in = ''
    if status:
       status = status.group(1)
    else:
        status = ''
    if distributions:
        distributions = distributions.group(1)
    else:
        distributions = ''
    if environemnt:
        environemnt = environemnt.group(1)
    else:
        environemnt = ''
    if taxonimic_scrutiny:
        taxonimic_scrutiny = taxonimic_scrutiny.group(1)
    else:
        taxonimic_scrutiny = ''
    if source_dataset:
        source_dataset = source_dataset.group(1)
    else:
        source_dataset = ''

    return f'{identifier}|{name}|{status}|{final_classification}|{distributions}|{environemnt}|{references}|{taxonimic_scrutiny}|{source_dataset}|{link_to_original}'

def get_visited_links():
    global visited
    temp = insect_visited_links.readlines()
    for link in temp:
        visited.append(link.replace('\n',''))
    insect_visited_links.close()

def visit_documents(driver):
    while len(queue) > 0:
        url = queue.pop(0)
        # url += "/keywords#keywords"
        if url in visited:
            continue
        driver.get(url)
        print("Visiting", url)
        time.sleep(0.3)
        wait = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH,"//div[@class='ant-row']")))
    

        raw_html = driver.find_element(By.XPATH, '//html').get_attribute('outerHTML')
        # extract info
        info = extract_info(raw_html, url)
        doc_id = url.split('/')[-1]
        single_file = open('data_insect/each_text/'+doc_id+'.txt','w', encoding='utf-8')
        single_file.write(raw_html)
        single_file.close()

        single_info_file = open('data_insect/each_text_info/'+doc_id+'_info.txt','w', encoding='utf-8')
        single_info_file.write(info)
        single_info_file.close()

        info_insect_concat.write(info+'\n')
        insect_raw.write(raw_html)
        insect_raw.write("\n<<END OF HTML>>\n")
        insect_raw.flush()

        insect_visited_links.write(url+'\n')
        insect_visited_links.flush()
        visited.append(url)
        #print(raw_html)

def get_links(driver, page_number):
    driver.get(f'https://www.catalogueoflife.org/data/search?TAXON_ID=H6&facet=rank&facet=issue&facet=status&facet=nomStatus&facet=nameType&facet=field&facet=authorship&facet=extinct&facet=environment&limit=50&offset={(page_number-1)*50}&rank=species&reverse=false&status=accepted&status=provisionally%20accepted')
     # keywords_button = driver.find_element(By.XPATH, "//div[@class='accordion-item'][.//button[@id='keywords']]")
    time.sleep(1)
    wait = WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.XPATH, '//td[@class="ant-table-cell"][.//a]')))
    # print(wait.get_attribute('innerHTML'))
    raw_html = driver.find_element(By.XPATH, '//html').get_attribute('outerHTML')
    # r'(?:href=")\/(document|book)\/(\d+)'
    paths_to_ids = re.findall(r'<a href=\"(\/data\/taxon\/[^\"]*)', raw_html)
    for path in paths_to_ids:
        url = f'https://www.catalogueoflife.org/{path}'
        if url not in queue and url not in visited:
            queue.append(url)
    print(queue)

def crawl():
    driver = selenium.webdriver.Edge()
    get_visited_links()
    insect_visited_pages = open("insect_visited_pages.txt", 'r', encoding='utf-8')
    page_number = 1
    try:
        page_number = int(insect_visited_pages.readlines()[-1])
        insect_visited_pages.close()
    except:
        page_number = 1
    insect_visited_pages = open("insect_visited_pages.txt", 'a', encoding='utf-8')
    print(page_number)
    global insect_visited_links
    insect_visited_links = open("insect_visited_links.txt", 'a', encoding='utf-8')
    while True:
        try:
            get_links(driver, page_number)
            visit_documents(driver)
            insect_visited_pages.write(str(page_number)+'\n')
            insect_visited_pages.flush()
            page_number += 1
        except KeyboardInterrupt:
            insect_raw.close()
            insect_visited_links.close()
            insect_visited_pages.close()
            info_insect_concat.close()
            break
   

    # print(raw_html)
    #print("This is body", wait.get_attribute('innerHTML'))
    # for item in listRes:
    #     print(item.get_attribute('innerHTML'))
    driver.quit()




crawl()
