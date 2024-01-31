import requests
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class RedditExtractor():

    def __init__(self, subreddit, listing_type):
        self.subreddit = subreddit
        self.listing_type = listing_type

    def set_headers(self):
        REDDIT_API_CLIENT_ID = os.getenv("CLIENT_ID")
        REDDIT_API_KEY = os.getenv("SECRET_KEY")
        REDDIT_USERNAME = os.getenv("REDDIT_USERNAME")
        REDDIT_PASSWORD = os.getenv("REDDIT_PASSWORD")

        auth = requests.auth.HTTPBasicAuth(REDDIT_API_CLIENT_ID, REDDIT_API_KEY)

        data = {
            'grant_type': 'password',
            'username': REDDIT_USERNAME,
            'password': REDDIT_PASSWORD
        }

        headers = {
            'User-Agent': 'TriangleHappeningsAPI/0.0.1'
        }

        TOKEN = self.obtain_access_token(auth, data, headers)

        headers['Authorization'] = f'bearer {TOKEN}'

        return headers

    def obtain_access_token(self, auth, data, headers):
        res = requests.post("https://www.reddit.com/api/v1/access_token", auth=auth, data=data, headers=headers)

        return res.json()['access_token']
    
    def load_reddit_data(self):

        headers=self.set_headers()
        url = f'https://oauth.reddit.com/r/{self.subreddit}/{self.listing_type}.json?sort=new'
        response = requests.get(url, headers=headers)

        return response.json()
    
    def build_reddit_dataframe(self, subreddit_data):
        links=[]
        content=[]
        metadata=[]

        for data in subreddit_data['data']['children']:
            links.append(data['data']['url'])
            content.append(data['data']['selftext'])
            metadata.append(data)

        df_dict = {
            'url': links,
            'content': content,
            'metadata': metadata
        }

        return pd.DataFrame(df_dict, columns=['url', 'content','metadata'])