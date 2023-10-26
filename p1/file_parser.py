import os

directory =  "C:\\Users\\tomas\\Desktop\FIIT\\FIIT ING SEM 1\\VINF\\p1\\data_ieee\\each_text\\"
def read_html():
    directory_new =  "C:\\Users\\tomas\\Desktop\FIIT\\FIIT ING SEM 1\\VINF\\p1\\data_ieee\\concat\\"
    concat_html = open(directory_new+'ieee_raw_html_new.txt', 'r', encoding='utf-8')
    line = concat_html.readline()
    print(line)

def check_length(filepath):
    file = open(filepath,'r', encoding='utf-8')
    number_of_lines = 0
    for line in file:
        number_of_lines += 1
    print("Number of lines", number_of_lines)
    return number_of_lines



def concat_html():
    directory_new =  "C:\\Users\\tomas\\Desktop\FIIT\\FIIT ING SEM 1\\VINF\\p1\\data_ieee\\concat\\"
    concat_html = open(directory_new+'ieee_raw_html_new2.txt', 'w', encoding='utf-8')
    for filename in os.listdir(directory):
        file = open(directory+filename, 'r', encoding='utf-8')
        print(filename)
        content = file.read()
        content = content.replace('\n', '')
        concat_html.write(content)
        concat_html.write('\n')
        concat_html.flush()
        file.close()
    concat_html.close()

#concat_html()
filepath = input("Filepath: ")
check_length(filepath)
