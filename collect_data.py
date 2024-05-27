from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs


if __name__ == "__main__":
    # Headers for request
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Accept-Language": "en-US, en;q=0.5",
    }

    books_links_list = []
    page_index = "page-1.html"
    base_url = "https://books.toscrape.com/"
    stop = False
    
    # Selecting pages
    
    while not stop:
        page_url = base_url+"catalogue/"+page_index
        page = requests.get(page_url, headers=HEADERS)
        if page.status_code == 200:
            soup = bs(page.content, "html.parser")

            list = soup.find("ol", class_="row")

            #get the next page
            pager = list.find_next("ul", class_ = "pager") 
            li = pager.find("li", class_ = "next")
            if li == None:
                stop = True
            else:
                page_index = li.find("a")["href"]

            books_links = list.find_all("h3")

            for h3 in books_links:
                a_elements = h3.find_all("a")
                for a in a_elements:
                    books_links_list.append(a["href"])
        else: 
            stop = True