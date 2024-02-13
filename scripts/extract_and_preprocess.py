import sys

new_path = "C:\\Users\\Brett\\OneDrive\\Desktop\\RTP-Radar\\"

if new_path not in sys.path:
    sys.path.append(new_path)

from ContentExtensions import *
from RssPull import *
from DatabaseInteractions import *

os.environ["HUGGINGFACEHUB_API_TOKEN"] = os.getenv["HUGGINGFACEHUB_API_TOKEN"]


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

    # Extract Nouns from content because we might want to use these to filter
    populated_content_df["content_nouns"] = extender.get_nouns(
        populated_content_df["content"]
    )

    # Extract Keywords from content because we might want to use these to display & filter
    populated_content_df["content_keywords"] = extender.get_keywords(
        populated_content_df["content"]
    )
    # Extract Emotions from content because we may want to display
    classifier = extender.set_hf_pipeline(
        "text-classification", "SamLowe/roberta-base-go_emotions"
    )

    populated_content_df["content_emotions"] = classifier(
        populated_content_df["content"].tolist()
    )

    # Generate summaries of content
    test_list = summarizer.get_summary(populated_content_df["content"])

    print(test_list)
