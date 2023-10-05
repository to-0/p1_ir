import requests
from bs4 import BeautifulSoup
import time

def crawl():
    raw_html = open('html_files_crawl.txt', 'a')
    pretty_html = open('pretty_html.txt', 'a')
    visited = open('visited_links.txt', 'a')
    url = "https://arxiv.org/abs/2311.03024"
    headers = {"Email": "xkalny@stuba.sk",
               "Purpose": "Academic purposes for subject at FIIT STU",
               "University": "Slovak Technical University",
               "Faculty": "Faculty of informatics and information technology",
               "Subject": "Information retrieval"}
    for year in range(23,7,-1):
        for month in range(12,1,-1):
            month_query = str(month).zfill(2)
            if year == 23 and month > 10:
                continue
            for id in range(1,9999):
                query_id = str(id).zfill(4)
                url = f'https://arxiv.org/abs/{year}{month}.{query_id}'
                response = requests.get(url, headers=headers)
                time.sleep(15.5)
                if response.status_code == 404:
                    visited.write(url+'\n')
                    break
                else:
                    visited.write(url+'\n')
                    content = response.content.decode('utf-8')
                    raw_html.write(content)
                    raw_html.write(f'\n<<<<END OF HTML>>>>\n')
                    raw_html.close()
                    soup = BeautifulSoup(content, 'html.parser')
                    pretty_html.write(soup.prettify())
                    pretty_html.write(f'\n<<<<END OF HTML>>>>\n')

    # do roku 2007 tam su zaznamy
    response = requests.get(url)
    print(response.status_code)

    #print(response.content.decode('utf-8'))


crawl()
