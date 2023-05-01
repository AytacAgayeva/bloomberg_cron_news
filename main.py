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
    title = url.find("news:title").text
    publication_date = url.find("news:publication_date").text[:10]
    publication_time = url.find("news:publication_date").text[11:19]
    image_loc_tag = url.find("image:loc")
    if image_loc_tag is not None:
        image_loc = image_loc_tag.text
    row_data = {
        "URL": loc,
        "Title": title,
        "Publication Date": publication_date,
        "Publication Time": publication_time,
        "Image URL": image_loc,
        }
    data.append(row_data)    
df = pd.DataFrame(data)
path="https://github.com/Aytage/bloomberg_cron_news/tree/main/json"
df.to_json(os.path.join(path,f'sitemap_news_{today}__{current_time}.json'))

data = [(pd.read_json(os.path.join(path, f)), f[-10:-5]) for f in os.listdir(path) if f.endswith(".json")]
news_count = pd.DataFrame({"Time": [time for _, time in data], "Count": [datas.shape[0] for datas, _ in data]})
old_data = None
old_time = None
old_file_name = None
news_data = []
for d, t in data:
    if old_data is not None:
        old = set(old_data['Title'])
        new = set(d['Title'])
        dif_old = old.difference(new)
        dif_new = new.difference(old)
        inter = old.intersection(new)
        news_time = old_time + ' - ' + t
        news_data.append({'Time': news_time, 'NEW': len(dif_new), 'SAME': len(inter), 'EXCLUDED': len(dif_old)})
    old_data = d
    old_time = t
all_news = pd.DataFrame({"Time": [d["Time"] for d in news_data], 
                          "NEW": [d["NEW"] for d in news_data], 
                          "SAME": [d["SAME"] for d in news_data], 
                          "EXCLUDED": [d["EXCLUDED"] for d in news_data]})
                          

