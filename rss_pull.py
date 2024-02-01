import feedparser
import requests
from bs4 import BeautifulSoup
import pandas as pd

feed_list = [
    # "http://www.wral.com/news/rss/142/",
    # "https://www.durhamnc.gov/RSSFeed.aspx?ModID=76&CID=All-0",
    # "https://abc11.com/feed/",
    "https://reddit.com/r/raleigh/new/.rss?sort=new",
    # "https://reddit.com/r/chapelhill/new/.rss?sort=new",
    # "https://reddit.com/r/bullcity/new/.rss?sort=new",
    # "https://www.townofchapelhill.org/Home/Components/RssFeeds/RssFeed/View?ctID=5&cateIDs=6",
    # "https://www.townofchapelhill.org/Home/Components/RssFeeds/RssFeed/View?ctID=5&deptIDs=15",
    # "https://www.townofchapelhill.org/Home/Components/RssFeeds/RssFeed/View?ctID=6&deptIDs=10"
    # "https://www.townofchapelhill.org/Home/Components/RssFeeds/RssFeed/View?ctID=5&cateIDs=1%2c2%2c6%2c8%2c9%2c12%2c14%2c15%2c18%2c71%2c72%2c73%2c74%2c86%2c105%2c108%2c117%2c120%2c124%2c193%2c199%2c204%2c261%2c274%2c277%2c286%2c289%2c290%2c291%2c292%2c293%2c294%2c296%2c299%2c301",
]

feed_all = []
authors = []
published = []
title = []
article_content = []

for source in feed_list:
    feed = feedparser.parse(source)
    published.extend(f"{item.published}" for item in feed.entries)
    authors.extend(f"{item.author}" for item in feed.entries)
    feed_all.extend(f"{item.link}" for item in feed.entries)

for url in feed_all:
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    description = soup.find_all("h1")
    item = soup.find_all("p")
    full_article_content = " ".join(
        [" ".join(text.get_text().split()) for text in item]
    )

    article_content.append(full_article_content)

    header = "".join(f"{content.get_text()}" for content in description)

    if "reddit" in url:
        title.append(f"{header}")
    else:
        title.append(f"{soup.title.get_text()}")

df_dict = {
    "published": published,
    "authors": authors,
    "feed_all": feed_all,
    "title": title,
    "article_content": article_content,
}

df = pd.DataFrame(
    df_dict, columns=["published", "authors", "feed_all", "title", "article_content"]
)

print(df.head())
