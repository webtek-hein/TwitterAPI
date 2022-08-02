from unittest.util import safe_repr
import tweepy
import config
from openpyxl import Workbook
import pandas as pd
import os


client = tweepy.Client(bearer_token=config.BEARER_TOKEN, wait_on_rate_limit=True)

def parse_result(tweets):
    result = []

    users = {u['id']: u["username"] for u in tweets.includes['users']}

    for tweet in tweets.data:
        result.append({
            "created_at": tweet.created_at.strftime("%d/%m/%Y %H:%M:%S"),
            "author": users[tweet.author_id],
            "conversation_id": f'{tweet.conversation_id}',
            "text": tweet.text,
            "in_reply_to": users.get(tweet.in_reply_to_user_id)
        })
    return result


def main(username, start_time):
    export_filename = "user_tweets_data.xlsx"
    uid = client.get_user(username=username).data.id
    all_tweets = []

    next_token = None

    while True:
        tweets = client.get_users_tweets(
            id=uid, 
            max_results=100,
            tweet_fields=["created_at", "conversation_id"],
            expansions=['author_id', 'in_reply_to_user_id'],
            pagination_token=next_token,
            start_time=start_time
        )
        all_tweets += parse_result(tweets)
    
        next_token = tweets.meta.get("next_token", False)

        if next_token == False:
            break

    
    replies = client.get_users_mentions(
        id=uid, max_results=100,
        tweet_fields=["created_at", "conversation_id"],
        expansions=['author_id', 'in_reply_to_user_id'],
        start_time=start_time
    )

    if not os.path.exists(export_filename):
        wb = Workbook()
        ws = wb.active
        ws.title = "Tweets"
        wb.save(export_filename)
    
    all_tweets_df = pd.DataFrame(all_tweets)
    user_tweets = all_tweets_df[~all_tweets_df["text"].str.startswith("RT")]
    user_retweets = all_tweets_df[all_tweets_df["text"].str.startswith("RT")]
    
    with pd.ExcelWriter(export_filename, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
        user_tweets.to_excel(writer, sheet_name="Tweets", index=False)
        user_retweets.to_excel(writer, sheet_name="Retweets", index=False)
        pd.DataFrame(parse_result(replies)).to_excel(writer, sheet_name="Replies", index=False)

if __name__ == "__main__":
    print("Twitter scrape!")
    main("mzfitzzz", "2021-09-01T00:00:00Z")