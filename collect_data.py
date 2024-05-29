from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine as ce

base_url = "https://books.toscrape.com/"

# Headers for request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept-Language": "en-US, en;q=0.5",
}

def getTitle(soup):
    try:
        bookTitle = soup.find("h1").text.strip()
    except AttributeError:
        bookTitle = ""
    return bookTitle

def getRate(soup):
    try:
        if soup.find("p", class_ = "star-rating One") != None:
            bookRate = 1
        elif soup.find("p", class_ = "star-rating Two")  != None:
            bookRate = 2
        elif soup.find("p", class_ = "star-rating Three")  != None:
            bookRate = 3
        elif soup.find("p", class_ = "star-rating Four")  != None:
            bookRate = 4
        elif soup.find("p", class_ = "star-rating Five")  != None:
            bookRate = 5
        else:
            bookRate = 0
    except AttributeError:
        bookRate = ""
    return bookRate

def getCategory(soup):
    try:
        breadcrumb = soup.find("ul", class_ = "breadcrumb")
        lis = breadcrumb.find_all("a")
        a = lis[2]
        category = a.text.strip()
    except AttributeError:
        category = ""
    return category

def getBooksLinksList():    

    books_links_list = []
    page_index = "page-1.html"
    
    stop = False
    
    # Get links for details of all books    
    while not stop:
        page_url = base_url+"catalogue/"+page_index
        page = requests.get(page_url, headers=HEADERS)
        if page.status_code == 200:
            soup = bs(page.content, "html.parser")

            list = soup.find("ol", class_="row")

            #get the next page
            pager = list.find_next("ul", class_ = "pager") 
            li = pager.find("li", class_ = "next")
            # if there is no next page, mark the stop flag to true to end the iteration
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
    return books_links_list

def getBooksInfoDict(booksLinksList):

    # Creating the dictionary schema
    result: Dict[str, List[str]] = {
        "UPC": [],
        "Title": [],
        "Category": [],
        "Product_Type": [],
        "Price(excl.tax)": [],
        "Price(incl.tax)": [],
        "Tax": [],
        "Stock": [],
        "Number_of_reviews": [],
        "Rating": [],
    }

    # Read a book info for each book link
    for link in booksLinksList:
        page_url = base_url+"catalogue/"+link
        page = requests.get(page_url, headers=HEADERS)
        if page.status_code == 200:
            soup = bs(page.content, "html.parser")
            top = soup.find("div", class_="col-sm-6 product_main")

            result["Title"].append(getTitle(top))
            result["Rating"].append(getRate(top))
            result["Category"].append(getCategory(soup))

            info = soup.find("table", class_ = "table table-striped")
            
            upc = info.find_next("td")
            result["UPC"].append(upc.text.strip())
            
            type = upc.find_next("td")
            result["Product_Type"].append(type.text.strip())

            price_excl_tax = type.find_next("td")
            result["Price(excl.tax)"].append(price_excl_tax.text.strip())

            price_incl_tax = price_excl_tax.find_next("td")
            result["Price(incl.tax)"].append(price_incl_tax.text.strip())

            tax = price_incl_tax.find_next("td")
            result["Tax"].append(tax.text.strip())

            avaiability = tax.find_next("td")
            result["Stock"].append(avaiability.text.strip())

            reviews = avaiability.find_next("td")
            result["Number_of_reviews"].append(reviews.text.strip())

    return result

def getBooksDataset(booksDict):
    # Creating pandas dataframe
    dataset_books = pd.DataFrame.from_dict(booksDict)
    
    # Here I extract the quantity in stock value from the text field containing the value.
    dataset_books['Stock'] = dataset_books['Stock'].str.extract(r'(\d+)')
    # Here I tranform the price from text like £10.00 to a numeric field
    dataset_books['Price(excl.tax)'] = dataset_books['Price(excl.tax)'].str.replace("£","")
    dataset_books['Price(excl.tax)'] = dataset_books['Price(excl.tax)'].astype(float)
    dataset_books['Price(incl.tax)'] = dataset_books['Price(incl.tax)'].str.replace("£","")
    dataset_books['Price(incl.tax)'] = dataset_books['Price(incl.tax)'].astype(float)

    dataset_books['Tax'] = dataset_books['Tax'].str.replace("£","")

    dataset_books.set_index('UPC')

    return dataset_books

def saveToCSV(dataset_books: pd.DataFrame):
    # Exporting dataset as .csv file
    dataset_books.to_csv("dataset_books.csv", index=False) 

def saveToDataBase(dataset_books: pd.DataFrame):
    
    engine = ce('postgresql://postgres:pgsql@localhost:5432/books')
    
    dataset_books.to_sql(name = "tbbooks", con = engine, if_exists= 'replace',index= False)
    

def main():
    books_links_list = getBooksLinksList() 
    #books_links_list = []
    #books_links_list.append("its-only-the-himalayas_981/index.html")   
    booksDict = getBooksInfoDict(books_links_list)
    dataset_books = getBooksDataset(booksDict)
    #dataset_books = pd.read_csv("dataset_books.csv")
    saveToCSV(dataset_books)
    saveToDataBase(dataset_books)
    

if __name__ == "__main__":
    main()

    

    
               

        