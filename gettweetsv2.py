from unittest.util import safe_repr
import tweepy
import config
from openpyxl import Workbook
import pandas as pd
import os
from datetime import datetime


class Listener(tweepy.StreamingClient):
    tweets = {
        "data": [],
        "includes": []
    }

    def on_data(self, tweets):
        users = {u['id']: u["username"] for u in tweets["includes"]["users"]}
        
        print({
            "created_at": tweets["data"]["created_at"].strftime("%d/%m/%Y %H:%M:%S"),
            "author": users[tweets["data"]["author_id"]],
            "conversation_id": f'{tweets["data"]["conversation_id"]}',
            "text": tweets["data"]["text"],
            "in_reply_to": users[tweets["data"]["in_reply_to_user_id"]]
        })
        

client = tweepy.Client(bearer_token=config.BEARER_TOKEN, wait_on_rate_limit=True)
stream = Listener(bearer_token=config.BEARER_TOKEN, wait_on_rate_limit=True)

def parse_result(tweets):
    print(tweets)
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

def get_all_tweets(uid, start_time):
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

    return all_tweets

def get_all_replies(uid, start_time):
    all_tweets = []
    next_token = None

    while True:
        tweets = client.get_users_mentions(
            id=uid, max_results=100,
            tweet_fields=["created_at", "conversation_id"],
            expansions=['author_id', 'in_reply_to_user_id'],
            start_time=start_time
        )

        all_tweets += parse_result(tweets)
    
        next_token = tweets.meta.get("next_token", False)

        if next_token == False:
            break

    return all_tweets

def create_tweet_file():
    wb = Workbook()
    ws = wb.active
    ws.title = "Tweets"
    wb.save(export_filename)

def main(username, start_time, export_filename):

    if not os.path.exists(export_filename):
        create_tweet_file()
    
        uid = client.get_user(username=username).data.id

        all_tweets_df = pd.DataFrame(get_all_tweets(uid, start_time))
        user_tweets = all_tweets_df[~all_tweets_df["text"].str.startswith("RT")]
        user_retweets = all_tweets_df[all_tweets_df["text"].str.startswith("RT")]

        with pd.ExcelWriter(export_filename, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
            user_tweets.to_excel(writer, sheet_name="Tweets", index=False)
            user_retweets.to_excel(writer, sheet_name="Retweets", index=False)
            pd.DataFrame(get_all_replies(uid, start_time)).to_excel(writer, sheet_name="Replies", index=False)

    # print(stream.get_rules())

    stream.add_rules(tweepy.StreamRule(f"from:{username} OR to:{username}"))
    stream.filter(
        tweet_fields=["created_at", "conversation_id"],
        expansions=['author_id', 'in_reply_to_user_id']
    )


if __name__ == "__main__":
    print("Twitter scrape!")
    username="scndryan"
    export_filename = "user_tweets_data.xlsx"
    start_time = "2021-09-01T00:00:00Z"
    main(username, start_time, export_filename)

