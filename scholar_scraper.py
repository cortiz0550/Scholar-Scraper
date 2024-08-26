import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random

def add_to_spreadsheet(df, name=""):
    df.to_excel(f"searched_articles_{name}.xlsx" , index=False)


# Use this to rotate user-agents while scraping.
# More UAs can be added.
def get_random_ua():
    options = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582',
        'Mozilla/5.0 (Linux; Android 13; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 13; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36',
        'Mozilla/5.0 (iPhone12,1; U; CPU iPhone OS 13_0 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/15E148 Safari/602.1',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1',
        'Mozilla/5.0 (Linux; Android 12; SM-X906C Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/80.0.3987.119 Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 11; Lenovo YT-J706X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
        'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0'
    ]

    return random.choice(options)


# Get the number of articles we expect for a search term.
def get_article_number(search_term, min_year, max_year, test=False):
    if test:
        return 10
    else:
        url = f'https://scholar.google.com/scholar?start=0&q={search_term}&hl=en&as_sdt=0,5&as_ylo={min_year}&as_yhi={max_year}'
        req = requests.get(url)
        soup = BeautifulSoup(req.content, "html.parser")
        # Only run this if the request is okay.
        if req.status_code == requests.codes.ok:
            number_text = soup.find_all("div", class_="gs_ab_mdw")[1].get_text()
            number = int(re.search(r'\d+(?:,\d+)?', number_text).group(0).replace(',', ''))
            return number
    
        # Raise the error if the request is invalid.
        else:
            print(soup.prettify())
            req.raise_for_status()
            return None


# Loop through number of articles and get the html from each page.
def get_articles(search_term, start_search=0, test=False):

    # This is an inner function meant to help us gather the data.
    def extract_article_info(article_list):
        titles = [article.h3.a.text if article.h3.a!=None else '[Unknown]' for article in article_list]
        all_titles.extend(titles)
        urls = [article.h3.a['href'] if article.h3.a!=None else '[Unknown]' for article in article_list]
        all_urls.extend(urls)
        authors = [article.find('div', attrs={'class': 'gs_a'}).get_text() for article in article_list]
        all_authors.extend(authors)
        abstracts = [article.find('div', attrs={'class':'gs_rs'}).get_text() for article in article_list]
        all_abstracts.extend(abstracts)


    # These will be the columns for our table 
    all_titles = []
    all_urls = []
    all_authors = []
    all_abstracts = []

    num_of_articles = get_article_number(search_term, test)
    print(f"Total number of articles: {num_of_articles}")

    if num_of_articles < 1:
        print("There are no articles for this search.")
        return None
    else:
        start = time.time()
        user_agent = get_random_ua()
        # Check to see if you get the final articles if we have a total thats a multiple of 10.
        for i in range(start_search, num_of_articles, 10):
            url = f'https://scholar.google.com/scholar?start={i}&q={search_term}&hl=en&as_sdt=0,5&as_ylo=2015&as_yhi=2024'
            ua_header = {
                "User-Agent": user_agent
            }

            tries = 0
            
            page = requests.get(url, headers=ua_header)
            print(f"Page: {(i//10) + 1} -- {page.status_code} -- Sec: {time.time() - start}")
            
            # Print the status code and wait for 20 min or raise code for other error.
            if page.status_code == 429:
                tries += 1
                if tries > 2:
                    break
                print("429 error raised. Waiting 20 min.")
                time.sleep(1200)
                user_agent = get_random_ua()
            elif page.status_code != 200:
                print(f"Status code: {page.status_code}")
                page.raise_for_status()
            
            # sleep between requests & pause every 30 pages
            if i % 250 == 0 and i > 0:
                print(f"***{i}th loop***")
                time.sleep(600)  # wait 10 min every 25 pages
            time.sleep(random.randint(10, 20))

            if i % 30 == 0:
                user_agent = get_random_ua()
            
            soup = BeautifulSoup(page.text, 'html.parser')
            article_names = soup.findAll('div', attrs={'class':'gs_r gs_or gs_scl'})
            
            # If the class above doesnt exist, we are probably being blocked even with a 200 code.
            # Sleep for 10 minutes and then try again.
            if not article_names:
                print("Google doesn't like that this is a bot.")
                break
                

            extract_article_info(article_names)

        print(f"{len(all_titles)} articles scraped.")
        return pd.DataFrame({'Title': all_titles,
                             'URL': all_urls,
                             'Author': all_authors,
                             'Abstract': all_abstracts
                             })
    

def clean_column_data(df, search_term):

    search_term = search_term.split()
    cleaned_search_term = [i.replace('"', '') for i in search_term]
    final_search_term = "_".join(cleaned_search_term)

    df["Author"] = df["Author"].astype(str)
    df["Year"] = df["Author"].str.extract(r"(\d{4})")
    # Fill missing values with 0
    df["Year"] = df["Year"].fillna(0).astype(int)

    df["Author(s)"] = df["Author"].str.split(n=1, pat="-").str[0]
    df["Search Term"] = final_search_term
    df = df.drop(columns=["Author"])
    return df
    
""" Program starts here """
     
search_terms = []

articles = []

# 1. Get articles and extract html
for st in search_terms:
    article_data = get_articles(st)
    
    # 2. Clean columns
    article_data = clean_column_data(article_data, st)
    if len(article_data) > 0:
        articles.append(article_data)

# 3. Create spreadsheet
final_df = pd.DataFrame(articles)
add_to_spreadsheet(final_df)