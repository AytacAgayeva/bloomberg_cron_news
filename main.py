import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date,datetime
import os, sys

import wget
url = 'https://www.bloomberg.com/feeds/sitemap_news.xml'
today=date.today()
now = datetime.now()
current_time = now.strftime("%H_%M")
name=f"sitemap_news_{today}__{current_time}.xml"
file_path=os.path.join("./xml",name)
filename = wget.download(url,file_path)

with open(filename, 'r',encoding='utf-8') as file:
    xml_content = file.read()
    
soup = BeautifulSoup(xml_content, 'xml')
urls = soup.find_all("url")
data = []
for url in urls:
    loc = url.loc.text
    title = ""
    language = ""
    publication_date = ""
    publication_time = ""
    keywords = ""
    image_loc = ""
    try:
        title = url.find("news:title").text
        language=url.find("news:language").text
        publication_date = url.find("news:publication_date").text[:10]
        publication_time = url.find("news:publication_date").text[11:19]
        image_loc = url.find("image:loc").text
        keywords=url.find("news:stock_tickers").text
    except AttributeError:
        pass
    
    row_data = {
        "URL": loc,
        "Title": title,
        "Language":language
        "Publication Date": publication_date,
        "Publication Time": publication_time,
        "Keywords":keywords,
        "Image URL": image_loc,
        }
    data.append(row_data)    
df = pd.DataFrame(data)
path="./json"
df.to_json(os.path.join(path,f'sitemap_news_{today}__{current_time}.json'))

files = [f for f in os.listdir(path) if f.endswith(".json")]
files.sort()
last_2_files=files[-2:]
if len(last_2_files)==2:
    data_new=pd.read_json(f'./json/{last_2_files[1]}')
    data_old=pd.read_json(f'./json/{last_2_files[0]}')
    news_count = pd.DataFrame({"Time": [last_2_files[1][-10:-5]], "Count": [data_new.shape[0]]})
    old = set(data_old['Title'])
    new = set(data_new['Title'])
    dif_old = old.difference(new)
    dif_new = new.difference(old)
    inter = old.intersection(new)
    all_news = pd.DataFrame({"Time": [f'{last_2_files[0][-10:-5]} - {last_2_files[1][-10:-5]}'], 
                          "NEW": [len(dif_new)], 
                          "SAME": [len(inter)], 
                          "EXCLUDED": [len(dif_old)]})
    all_news.to_csv(f'./all_news/sitemap_news_{today}__{current_time}.csv',index=False)
    news_count.to_csv(f'./news_count/sitemap_news_{today}__{current_time}.csv',index=False)
    
    files_news = [f for f in os.listdir("./all_news") if f.endswith(".csv")]
    files_news.sort()
    file_path = os.path.join("./all_news", files_news[0])
    data_all = pd.read_csv(file_path)
    data_all=pd.concat([data_all,all_news],axis=0)
    data_all.to_csv(file_path,index=False)
    
    count_news = [f for f in os.listdir("./news_count") if f.endswith(".csv")]
    count_news.sort()
    count_path = os.path.join("./news_count", count_news[0])
    count_all = pd.read_csv(count_path)
    count_all=pd.concat([count_all,news_count],axis=0)
    count_all.to_csv(count_path,index=False)
    
else:
    data_old=pd.read_json(f'./json/{last_2_files[0]}')
    news_count = pd.DataFrame({"Time": [last_2_files[0][-10:-5]], "Count": [data_old.shape[0]]})
    all_news_ = pd.DataFrame({"Time": [f'{last_2_files[0][-10:-5]}'], 
                          "NEW": [data_old.shape[0]], 
                          "SAME": [0], 
                          "EXCLUDED": [0]})
    all_news_.to_csv(f'./all_news/all_time_news.csv',index=False)
    news_count.to_csv(f'./news_count/all_time_news_count.csv',index=False)
