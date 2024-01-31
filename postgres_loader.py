from news_article_extraction import NewsExtractor
from reddit_extraction import RedditExtractor

subreddits = ['raleigh', 'chapelhill', 'bullcity']
listings = ['new']
newssites = ['wral']


for site in newssites:
    parser = NewsExtractor(site)
    homepage = parser.get_homepage()
    links = parser.extract_article_links(homepage)
    df = parser.build_article_dataframe(links)


for subreddit in subreddits:
    for listing in listings:
        raleigh=RedditExtractor(subreddit, listing)
        raleigh_data=raleigh.load_reddit_data()
        raleigh_df=raleigh.build_reddit_dataframe(raleigh_data)
        raleigh_df.to_csv(f'{subreddit}_reddit_data.csv')