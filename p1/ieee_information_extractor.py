import re
import os
import csv
extracted = {}
no_content_counter = 0
shortened_counter = 0
def load_extracted():
    global extracted
    try:
        f = open("info_extraction_history\\ieee_extracted_info_documents.txt", 'r')
    except:
        return
    for line in f:
        extracted[line] = True
    f.close()


def write_raw_to_info_concat(rewrite=True):
    no_abs = False
    final_name = input("Write the name of the resulting csv_file: ")
    contain_no_abstract = input("Filter out documents with no abstract? y/n: ")
    if contain_no_abstract == 'y':
        no_abs = True
    dir = "C:\\Users\\tomas\\Desktop\FIIT\\FIIT ING SEM 1\\VINF\\p1\\data_ieee\\concat\\"
    mode = 'w+'
    if rewrite:
        mode = 'w+'
        last_line = 0
    else:
        try:
            f = open("info_extraction_history\\ieee_extracted_info_last_line.txt", 'r')
            last_line = int(f.readlines()[-1])
            f.close()
            mode = 'a+'
        except:
            mode= 'w+'
            last_line = 0
    finaal_file = open(f'{final_name}', mode, newline='', encoding='utf-8')
    writer = csv.writer(finaal_file, delimiter='\t')
    header = 'link\ttitle\tauthor\tcontent\tpublisher\tyear\tpages\tieee_keys\tauthor_keys'
    header = header.split('\t')
    writer.writerow(header)
    concat_html = open(f'{dir}ieee_raw_html_new.txt', 'r', encoding='utf-8')
    for i in range(last_line+1):
        concat_html.readline()
    count = 1
    while True:
        line = concat_html.readline()
        if not line:
            print("not line")
            break
        print(count)
        info = extract_info(line)
        info = info.split('\t')
        # filter out documents with no content
        if no_abs and len(info[3]) == 0:
            continue
        writer.writerow(info)
        count += 1
    f = open('info_extraction_history\\ieee_extracted_info_last_line.txt', mode)
    last_line += count
    f.write(str(last_line))
    f.write('\n')
    f.close()
    print("Documents with no abstract content:", no_content_counter)
    print("Shortened counter ", shortened_counter)

    finaal_file.close()

## DEPRECATED 
def write_raw_info_from_separate_to_concat(rewrite=True):
    no_abs = False
    final_name = input("Write the name of the resulting csv_file: ")
    contain_no_abstract = input("Filter out documents with no abstract? y/n")
    if contain_no_abstract == 'y':
        no_abs = True
    dir = "C:\\Users\\tomas\\Desktop\FIIT\\FIIT ING SEM 1\\VINF\\p1\\data_ieee\\each_text\\"
    files = os.listdir(dir)
    count = 0
    if not rewrite:
        load_extracted()
        mode = 'a+'
    else:
        mode = 'w+'
    visited = open('info_extraction_history\\ieee_extracted_info_documents.txt', mode, encoding='utf-8')
    header = 'link\ttitle\tauthor\tcontent\tpublisher\tyear\tpages\tieee_keys\tauthor_keys'    
    finaal_file = open(f'{final_name}1', mode, newline='', encoding='utf-8')
    # Write header
    writer = csv.writer(finaal_file, delimiter='\t')
    writer.writerow(header)
    for filename in files:
        if not rewrite and extracted.get(filename) is not None:
            continue
        print(count)
        print(filename)
        file = open(dir+filename, 'r', encoding='utf-8')
        raw_html = file.read()
        info = extract_info(raw_html)
        info = info.split('\t')
         # filter out documents with no content
        if no_abs and len(info[3]) == 0:
            continue
        writer.writerow(info)
        count += 1
        file.close()
        visited.write(filename)
        visited.write('\n')
    print("Documents with no abstract content:", no_content_counter)
    print("Shortened counter ", shortened_counter)
    visited.close()
    finaal_file.close()


def extract_info(raw_html):
    global no_content_counter
    global shortened_counter
    raw_html= raw_html.replace('\t', ' ')
    raw_html = raw_html.strip()
    # <meta name="parsely-link" content="https://ieeexplore.ieee.org/document/9282637">
    link = re.search(r'<meta name="parsely-link" content="([^"]*)', raw_html)
    if link:
        link = link.group(1)
    else:
        link = ''

    title = re.search(r'<h1.+?(?:document-title).+?><span[^>]*>(.+?)<', raw_html)
    authors = re.search(r'<meta\s+name="parsely-author"\s+content="([^"]*)"', raw_html)

    #content = re.search(r'class="abstract-text.*?>?.*<\/strong><div[^>]*>(.*?)<', raw_html)
    content = re.search(r'<meta property="og:description".*?content="([^"]*)"', raw_html)

    pages = re.search(r'Page\(s\): <\/strong>\s*([0-9]*)', raw_html)
    publisher = re.search(r'Publisher:\s*<\/span><!----><span .*?>(.*?)<', raw_html)
    year = re.search(r'(?:Date of Publication:|Copyright Year:)\s*.*?>\s*([^<]*)', raw_html)
    if title:
        title = title.group(1)
    else:
        title = ''
    
    if authors:
        authors = authors.group(1)
    else:
        authors = ''
    
    if content:
        content = content.group(1)
        if content[-1] == '.' and content[-2] == '.' and content[-3] == '.':
            shortened_counter += 1
    else:
        content = ''
        no_content_counter += 1
    
    if pages:
        pages = pages.group(1)
    else: 
        pages = ''
    
    if year:
        year = year.group(1)
    else:
        year = ''
    
    if publisher:
        publisher = publisher.group(1)
    else:
        publisher = ''
    


    keywords_ieee_ul = re.search(r'>IEEE\sKeywords.*?<ul[^>]*>(.*?)<\/ul>', raw_html)
    if keywords_ieee_ul:
        keywords_ieee_ul = keywords_ieee_ul.group(1)
    else:
        keywords_ieee_ul = None

    if keywords_ieee_ul is not None:
        keywords_ieee = re.findall(r'<li[^>]*><a[^>]*>(.*?)<\/a>', keywords_ieee_ul)
    else:
        keywords_ieee = 'Not found'

    author_keywords_ul = re.search(r'>Author[\(s\)]*\sKeywords.*?<ul[^>]*>(.*?)<\/ul>', raw_html)
    if author_keywords_ul:
        author_keywords_ul = author_keywords_ul.group(1)
    else:
        author_keywords_ul = None

    if author_keywords_ul is not None:
        keywords_author = re.findall(r'<li[^>]*><a[^>]*>(.*?)<\/a>', author_keywords_ul)
    else:
        keywords_author = 'Not found'

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
    return f'{link}\t{title}\t{authors}\t{content}\t{publisher}\t{year}\t{pages}\t{ieee_keys}\t{author_keys}'

# test_csv_writer()
# read_test()
import os
def main():
    if not os.path.exists('info_extraction_history'):
        os.mkdir('info_extraction_history')
    opt = '2'
    rewrite = input("Rewrite existing files? y/n")
    if rewrite == "y":
        rewrite = True
    else:
        rewrite = False
    if opt == "1": 
       load_extracted()
       write_raw_info_from_separate_to_concat(rewrite)
    else:
        write_raw_to_info_concat(rewrite)

main()
#write_raw_info_from_separate_to_separate()