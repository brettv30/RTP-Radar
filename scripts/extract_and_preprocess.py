import sys

new_path = "C:\\Users\\Brett\\OneDrive\\Desktop\\RTP-Radar\\"

if new_path not in sys.path:
    sys.path.append(new_path)

from RssPull import *
from DatabaseInteractions import *

# Script used for postgres table extraction and data preprocessing
if __name__ == "__main__":
    pg_server = DatabaseManipulate("database.ini", "postgresql")
    preprocessor = DataCleaner()

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

    populated_content_df = preprocessor.filter_for_populated_content(cleaned_content_df)

    # Filter for only news observations, that way we can identify the news that is actually about RTP
    only_news = populated_content_df[
        ~populated_content_df["url"].str.contains("reddit|dailytarheel", case=False)
    ]

    print(only_news["content"].head(10))
