import os

directory =  "C:\\Users\\tomas\\Desktop\FIIT\\FIIT ING SEM 1\\VINF\\data_ieee\\each_text\\"
def read_html():
    directory_new =  "C:\\Users\\tomas\\Desktop\FIIT\\FIIT ING SEM 1\\VINF\\data_ieee\\concat\\"
    concat_html = open(directory_new+'ieee_raw_html_new.txt', 'r', encoding='utf-8')
    line = concat_html.readline()
    print(line)


def concat_html():
    directory_new =  "C:\\Users\\tomas\\Desktop\FIIT\\FIIT ING SEM 1\\VINF\\data_ieee\\concat\\"
    concat_html = open(directory_new+'ieee_raw_html_new.txt', 'w', encoding='utf-8')
    for filename in os.listdir(directory):
        file = open(directory+filename, 'r', encoding='utf-8')
        print(filename)
        content = file.read()
        content = content.replace('\n', '')
        concat_html.write(content)
        concat_html.write('\n')
    concat_html.close()

concat_html()