from RssPull import *
from DatabaseInteractions import *

# Script used for postgres table extraction and data preprocessing
if __name__ == "__main__":
    pg_server = DatabaseInteractions.DatabaseManipulate("database.ini", "postgresql")
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

    print(populated_content_df.head())
    print(populated_content_df.tail())

    # Note here to think about how much preprocessing we want to do on the content.
    # For example, we could remove stop words, punctuation, and other non-essential characters.
    # We also could exclude any observations that have content which is not relevant to raligh, durham, chapel hill, or the RTP area
