import requests
import json
import pandas as pd
from bs4 import BeautifulSoup

class NewsExtractor():

    def __init__(self, news_site):
        self.news_site = news_site
        self.url = f"https://www.{news_site}.com"

    def get_homepage(self):
        response = requests.get(self.url)
        return BeautifulSoup(response.text, 'html.parser')

    def extract_article_links(self, homepage):
        links = []
        for link in homepage.find_all('a'):
            if self.is_article_link(link, links):

                if (self.url not in link.get('href')) and ("https://" not in link.get('href')):
                    full_url = self.url + link.get('href')
                else:
                    full_url = link.get('href')

                links.append(full_url)
        return links

    def is_article_link(self, link, links):
        # Implement site-specific logic to check if link is an article
        if (link.get('href') is not None):

            if (self.url not in link.get('href')) and ("https://" not in link.get('href')):
                full_url = self.url + link.get('href')
            else:
                full_url = link.get('href')

            # if (full_url not in links) and (full_url.count('/') > 4) and ("-" in full_url):
            if (full_url not in links):
                return True
                
            return False

    def extract_article_content(self, article_url):
        response = requests.get(article_url)
        i = 1
        for p in BeautifulSoup(response.text, 'html.parser').find_all('p'):
            if i == 1:
                return p.get_text()

    def extract_article_metadata(self, article_url):
        # Extract and return metadata
        page = requests.get(article_url)
        articles = BeautifulSoup(page.text,"html.parser")
        # Extract the description, date published, and the URL from the below output
        if articles.find('script', {'type': 'application/ld+json'}) is not None:
            return json.loads(articles.find('script', {'type': 'application/ld+json'}).text, strict=False)
        
    def build_article_dataframe(self, links):
        # Extract article links, content, and metadata
        slim_links = []
        content = []
        metadata = []

        for link in links:
            # Extract actual articles from the links returned from wral extraction
            if "/story/" in link:
                slim_links.append(link)
                content.append(self.extract_article_content(link))
                metadata.append(self.extract_article_metadata(link))

        # Build a dataframe with the article links, content, and metadata
        df_dict = {
            'url': slim_links,
            'content': content,
           'metadata': metadata
        }
        return pd.DataFrame(df_dict, columns=['url', 'content','metadata'])