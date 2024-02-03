import logging
from urllib.parse import urlparse
import concurrent.futures
from bs4 import BeautifulSoup
import feedparser
import requests
from datetime import datetime
import pandas as pd
from contextlib import contextmanager
import time as time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@contextmanager
def timer(label):
    start = time.time()
    try:
        yield
    finally:
        end = time.time()
    time = round(end - start, 2)
    logger.info(f"{label}: {time}, seconds")


def parse_feed(feed_url):
    return feedparser.parse(feed_url)


def parse_page(page):
    return BeautifulSoup(page, "html.parser")


def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def extract_content(url):
    if is_valid_url(url):
        page = requests.get(url).text
        soup = parse_page(page)
        item = soup.find_all("p")
        return " ".join([" ".join(text.get_text().split()) for text in item])
    else:
        logger.warning(f"Invalid URL - Content: {url}")
        return None


def extract_titles(url):
    if is_valid_url(url):
        page = requests.get(url).text
        soup = parse_page(page)
        description = soup.find_all("h1")
        header = "".join(f"{content.get_text()}" for content in description)
        title = f"{header}" if "reddit" in url else f"{soup.title.get_text()}"
        return title
    else:
        logger.warning(f"Invalid URL - Titles: {url}")
        return None


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


def clean_titles(title_list):
    replace_newlines = replace_newlines_and_slashes(title_list)

    # Processing the list to remove " - " at the end of each string
    replace_suffixes = [title.rstrip(" - ") for title in replace_newlines]

    # Remove leading and trailing whitespace from each element
    return [s.strip() for s in replace_suffixes]


def replace_newlines_and_slashes(data_list):
    # Replace any instances of \n or \' in the title with nothing or ', respectively
    return [
        info.replace("\n", "").replace("\\'", "'").replace("\\\\'", "'")
        for info in data_list
    ]


def clean_content(content_list):
    replace_newlines = replace_newlines_and_slashes(content_list)

    # List of prefixes
    prefixes = [
        "Chapel Hill, NC",
        "A subreddit for the city (and county) of Durham, North Carolina.",
        'Raleigh is the capital of the state of North Carolina as well as the seat of Wake County. Raleigh is known as the "City of Oaks" for its many oak trees. Join us on Discord! https://discord.gg/PPCARNjJAg',
    ]

    # Iterate through each element and check for prefixes
    for i, element in enumerate(replace_newlines):
        for prefix in prefixes:
            if element.startswith(prefix):
                # Replace the prefix with whitespace
                replace_newlines[i] = element.replace(
                    prefix, "", 1
                )  # Replace only the first occurrence
                break  # Stop checking other prefixes if one has already matched

    return [s.strip() for s in replace_newlines]


feed_list = [
    "http://www.wral.com/news/rss/142/",
    "https://www.durhamnc.gov/RSSFeed.aspx?ModID=76&CID=All-0",
    # "https://abc11.com/feed/",
    "https://www.dailytarheel.com/plugin/feeds/tag/pageOne"
    "https://reddit.com/r/raleigh/new/.rss?sort=new",
    # "https://reddit.com/r/chapelhill/new/.rss?sort=new",
    # "https://reddit.com/r/bullcity/new/.rss?sort=new",
]

all_urls = []
published = []
authors = []
titles = []
article_content = []

with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(parse_feed, url) for url in feed_list]

    for future in concurrent.futures.as_completed(futures):
        feed = future.result()

        for item in feed.entries:
            with timer("Extracting publish date"):
                if hasattr(item, "published"):
                    published.append(item.published)
                else:
                    published.append("Unknown date")

            with timer("Extracting authors"):
                try:
                    authors.append(item.author)
                except AttributeError:
                    authors.append("Unknown")

            with timer("Extracting URLs"):
                if hasattr(item, "link"):
                    all_urls.append(item.link)
                else:
                    all_urls.append("Unknown link")

            with timer("Extracting titles"):
                if hasattr(item, "title"):
                    titles.append(item.title)
                else:
                    title_results = extract_titles(item.link)
                    if title_results is not None:
                        titles.append(title_results)

            with timer("Extracting content"):
                if hasattr(item, "content"):
                    article_content.append(item.content)
                else:
                    content_results = extract_content(item.link)
                    if content_results is not None:
                        article_content.append(content_results)

initial_daily_df = build_dataframe(
    published, authors, all_urls, titles, article_content
)

print(initial_daily_df.head())
print(initial_daily_df.tail())
