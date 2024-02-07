import sys

new_path = "C:\\Users\\Brett\\OneDrive\\Desktop\\RTP-Radar\\"

if new_path not in sys.path:
    sys.path.append(new_path)

from RssPull import *
from DatabaseInteractions import *

# Main script used to pull the RSS feeds in feed_list
if __name__ == "__main__":
    feed_list = [
        "http://www.wral.com/news/rss/142/",
        "https://www.durhamnc.gov/RSSFeed.aspx?ModID=76&CID=All-0",
        "https://abc11.com/feed/",
        "https://www.dailytarheel.com/plugin/feeds/tag/pageOne"
        "https://reddit.com/r/raleigh/new/.rss?sort=new",
        "https://reddit.com/r/chapelhill/new/.rss?sort=new",
        "https://reddit.com/r/bullcity/new/.rss?sort=new",
    ]

    pg_server = DatabaseManipulate("database.ini", "postgresql")
    preprocessor = DataCleaner()

    new_rss_pull = RssPull(feed_list)
    rss_feed_data = new_rss_pull.pull_feed()

    df_cols = list(rss_feed_data.keys())
    initial_df = pd.DataFrame(rss_feed_data, columns=df_cols)

    cleaned_dates_df = preprocessor.clean_published_dates(initial_df)

    last_24_df = preprocessor.filter_for_last_24_hrs(cleaned_dates_df)

    load_to_pg = last_24_df[
        [
            "extracted_date",
            "formatted_eastern_published",
            "urls",
            "authors",
            "title",
            "content",
        ]
    ].copy()

    load_to_pg.rename(
        columns={
            "extracted_date": "extraction_date",
            "formatted_eastern_published": "published_date",
            "urls": "url",
            "authors": "author",
        },
        inplace=True,
    )

    # Clear table while testing
    pg_server.run_ddl_commands(["""TRUNCATE TABLE land_tbl_raw_feeds"""])

    # Load Raw Data from the last 24 hours into postgres DB
    pg_server.insert_pd_dataframe(load_to_pg, "land_tbl_raw_feeds")
