import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date,datetime

import wget
url = 'https://www.bloomberg.com/feeds/sitemap_news.xml'
now = datetime.now()
current_time = now.strftime("%H_%M")
name=f"sitemap_news_{today}__{current_time}.xml"
filename = wget.download(url, name)
