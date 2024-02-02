import feedparser
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

feed_list = [
    "http://www.wral.com/news/rss/142/",
    # "https://www.durhamnc.gov/RSSFeed.aspx?ModID=76&CID=All-0",
    # "https://abc11.com/feed/",
    # "https://www.dailytarheel.com/plugin/feeds/tag/pageOne"
    # "https://reddit.com/r/raleigh/new/.rss?sort=new",
    # "https://reddit.com/r/chapelhill/new/.rss?sort=new",
    # "https://reddit.com/r/bullcity/new/.rss?sort=new",
]

all_urls = []
authors = []
published = []
titles = []
article_content = []

for source in feed_list:
    feed = feedparser.parse(source)
    published.extend(f"{item.published}" for item in feed.entries)

    try:
        authors.extend(f"{item.author}" for item in feed.entries)
    except AttributeError:
        authors.extend("Unknown")

    all_urls.extend(f"{item.link}" for item in feed.entries)

# count = 0
for url in all_urls:
    # if count == 3:
    #     break
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
        titles.append(f"{header}")
    else:
        titles.append(f"{soup.title.get_text()}")

#  count += 1


def build_dataframe(published, authors, all_urls, title, article_content):
    df_dict = {
        "published": published,
        "authors": authors,
        "urls": all_urls,
        "title": title,
        "article_content": article_content,
    }

    return pd.DataFrame(
        df_dict, columns=["published", "authors", "urls", "title", "article_content"]
    )


def clean_published_dates(date_list):
    # Define the date format
    date_format = "%a, %d %b %Y %H:%M:%S %z"

    # Parse each string into a datetime object
    parsed_dates = [
        datetime.strptime(date_string, date_format) for date_string in date_list
    ]

    # Convert datetime objects to strings in the desired format
    return [date.strftime("%Y-%m-%d %H:%M:%S") for date in parsed_dates]


print(authors)
print(titles)
