import logging
from urllib.parse import urlparse
import concurrent.futures
from bs4 import BeautifulSoup
import feedparser
import requests
from datetime import datetime
import pandas as pd
from contextlib import contextmanager
import time as tme

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@contextmanager
def timer(label):
    start = tme.time()
    try:
        yield
    finally:
        end = tme.time()
    time = round(end - start, 2)
    logger.info(f"{label}: {time} seconds")


class DataTransformer:
    def __init__(self):
        self.date_format = "%a, %d %b %Y %H:%M:%S %z"
        self.reddit_content_prefixes = [
            "Chapel Hill, NC",
            "A subreddit for the city (and county) of Durham, North Carolina.",
            'Raleigh is the capital of the state of North Carolina as well as the seat of Wake County. Raleigh is known as the "City of Oaks" for its many oak trees. Join us on Discord! https://discord.gg/PPCARNjJAg',
        ]

    def clean_published_dates(self, dataframe):
        date_list = dataframe["published"]
        # Parse each string into a datetime object
        parsed_dates = [
            datetime.strptime(date_string, self.date_format)
            for date_string in date_list
        ]
        # Convert datetime objects to strings in the desired format
        dataframe["cleaned_published"] = [
            date.strftime("%Y-%m-%d %H:%M:%S") for date in parsed_dates
        ]

        return dataframe

    def clean_titles(self, dataframe):
        title_list = dataframe["title"]

        replace_newlines = self.replace_newlines_and_slashes(title_list)

        # Processing the list to remove " - " at the end of each string
        replace_suffixes = [title.rstrip(" - ") for title in replace_newlines]

        # Remove leading and trailing whitespace from each element
        dataframe["cleaned_title"] = [s.strip() for s in replace_suffixes]

        return dataframe

    def replace_newlines_and_slashes(self, data_list):
        # Replace any instances of \n or \' in the title with nothing or ', respectively
        return [info.replace("\n", "").replace("\\'", "'") for info in data_list]

    def clean_content(self, dataframe):
        content_list = dataframe["content"]

        replace_newlines = self.replace_newlines_and_slashes(content_list)

        # Iterate through each element and check for prefixes
        for i, element in enumerate(replace_newlines):
            for prefix in self.reddit_content_prefixes:
                if element.startswith(prefix):
                    # Replace the prefix with whitespace
                    replace_newlines[i] = element.replace(
                        prefix, "", 1
                    )  # Replace only the first occurrence
                    break  # Stop checking other prefixes if one has already matched

        dataframe["cleaned_content"] = [s.strip() for s in replace_newlines]

        return dataframe


class URLParser:
    def __init__(self) -> None:
        pass

    def parse_page(self, page):
        return BeautifulSoup(page, "html.parser")

    def is_valid_url(self, url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False

    def extract_url_content(self, url):
        if self.is_valid_url(url):
            page = requests.get(url).text
            soup = self.parse_page(page)
            item = soup.find_all("p")
            return " ".join([" ".join(text.get_text().split()) for text in item])
        else:
            logger.warning(f"Invalid URL - Content: {url}")
            return None

    def extract_url_title(self, url):
        if self.is_valid_url(url):
            page = requests.get(url).text
            soup = self.parse_page(page)
            description = soup.find_all("h1")
            header = "".join(f"{content.get_text()}" for content in description)
            title = f"{header}" if "reddit" in url else f"{soup.title.get_text()}"
            return title
        else:
            logger.warning(f"Invalid URL - Titles: {url}")
            return None


class RssPull:
    def __init__(self, url_parser, feed_list):
        self.url_parser = url_parser
        self.feed_list = feed_list

    def parse_feed(self, url):
        return feedparser.parse(url)

    def pull_feed(self):
        all_urls = []
        all_published_dates = []
        all_authors = []
        all_titles = []
        all_content = []

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.parse_feed, url) for url in self.feed_list]

            for future in concurrent.futures.as_completed(futures):
                feed = future.result()

                for item in feed.entries:
                    all_published_dates.append(self.extract_published_date(item))
                    all_authors.append(self.extract_authors(item))
                    all_urls.append(self.extract_urls(item))
                    all_titles.append(self.extract_titles(item))
                    all_content.append(self.extract_content(item))

        return {
            "published": all_published_dates,
            "authors": all_authors,
            "urls": all_urls,
            "title": all_titles,
            "content": all_content,
        }

    def extract_published_date(self, item):
        with timer("Extracting publish date"):
            return item.published if hasattr(item, "published") else "Unknown date"

    def extract_authors(self, item):
        with timer("Extracting authors"):
            return item.author if hasattr(item, "author") else "Unknown author"

    def extract_urls(self, item):
        with timer("Extracting URLs"):
            return item.link if hasattr(item, "link") else "Unknown link"

    def extract_titles(self, item):
        with timer("Extracting titles"):
            if hasattr(item, "title"):
                return item.title
            title_results = self.url_parser.extract_url_title(item.link)
            return title_results if title_results is not None else "Unknown title"

    def extract_content(self, item):
        with timer("Extracting content"):
            if hasattr(item, "content") and "reddit" in item.link:
                # Extracting the HTML part from your list element
                html_content = item.content[0]["value"]

                # Parsing the HTML with BeautifulSoup
                soup = self.url_parser.parse_page(html_content)

                # Extracting text from all <p> tags if there are any <p> tags
                if len(soup.find_all("p")) > 0:
                    for p in soup.find_all("p"):
                        content = p.get_text()
                else:
                    content = ""

                return content
            elif (
                hasattr(item, "content")
                and "abc11" in item.link
                or not hasattr(item, "content")
            ):
                content_results = self.url_parser.extract_url_content(item.link)
                return content_results if content_results is not None else ""
            else:
                return item.content


feed_list = [
    "http://www.wral.com/news/rss/142/",
    "https://www.durhamnc.gov/RSSFeed.aspx?ModID=76&CID=All-0",
    "https://abc11.com/feed/",
    "https://www.dailytarheel.com/plugin/feeds/tag/pageOne"
    "https://reddit.com/r/raleigh/new/.rss?sort=new",
    "https://reddit.com/r/chapelhill/new/.rss?sort=new",
    "https://reddit.com/r/bullcity/new/.rss?sort=new",
]


parser = URLParser()

test = RssPull(parser, feed_list)
test_dict = test.pull_feed()

dict_cols = list(test_dict.keys())
initial_df = pd.DataFrame(test_dict, columns=dict_cols)

preprocessor = DataTransformer()

test_df = preprocessor.clean_published_dates(initial_df)
print(test_df.info())

test_df2 = preprocessor.clean_titles(test_df)
print(test_df2.info())

test_df3 = preprocessor.clean_content(test_df2)
print(test_df3.info())
