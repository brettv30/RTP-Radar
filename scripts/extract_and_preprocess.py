import sys
import concurrent.futures

new_path = "C:\\Users\\Brett\\OneDrive\\Desktop\\RTP-Radar\\"

if new_path not in sys.path:
    sys.path.append(new_path)

from ContentExtensions import *
from RssPull import *
from DatabaseInteractions import *

# Script used for postgres table extraction and data preprocessing
if __name__ == "__main__":
    pg_server = DatabaseManipulate("database.ini", "postgresql")
    preprocessor = DataCleaner()
    extender = ContentExtender()
    summarizer = ContentSummarizer()

    # Get the most recent data from the table
    extraction_query = """ SELECT *
                            FROM land_tbl_raw_feeds
                            WHERE extraction_date = (SELECT MAX(extraction_date) FROM land_tbl_raw_feeds)
                        """

    columns_to_extract = [
        "table_id",
        "extraction_date",
        "published_date",
        "url",
        "author",
        "title",
        "content",
    ]

    # Extract all Raw Data from the last 24 hours from postgres DB
    pg_raw = pg_server.pg_to_pd_dataframe(extraction_query, columns_to_extract)

    # Additional preprocessing of raw data
    cleaned_titles_df = preprocessor.clean_titles(pg_raw)
    cleaned_content_df = preprocessor.clean_content(cleaned_titles_df)

    # Make an actual dataframe instead of a slice
    populated_content_df = preprocessor.filter_for_populated_content(
        cleaned_content_df
    ).copy()

    # Using ThreadPoolExecutor to parallelize the processing of entire content lists
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # Submit the entire list to each function
        future_nouns = executor.submit(
            extender.get_nouns, populated_content_df["content"]
        )
        future_keywords = executor.submit(
            extender.get_keywords, populated_content_df["content"]
        )
        future_emotions = executor.submit(
            extender.classify_emotions, populated_content_df["content"].tolist()
        )
        future_summaries = executor.submit(
            summarizer.get_summaries, populated_content_df["content"]
        )

        # Wait for all futures to complete and assign the results
        populated_content_df["content_nouns"] = future_nouns.result()
        populated_content_df["content_keywords"] = future_keywords.result()
        populated_content_df["content_emotions"] = future_emotions.result()
        populated_content_df["content_summaries"] = future_summaries.result()

    print(populated_content_df["content"].head())
    print(populated_content_df["content_nouns"].head())
    print(populated_content_df["content_keywords"].head())
    print(populated_content_df["content_emotions"].head())
    print(populated_content_df["content_summaries"].head())
