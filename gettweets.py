from email import header
from re import I
from unittest import result
from pip import main
import requests
import config
import pandas as pd

HEADER = {
    "Authorization": config.BEARER_TOKEN,
    "consumer_key": config.API_KEY, 
    "consumer_secret": config.API_SECRET,
    "access_token": config.ACCESS_TOKEN,
    "token_secret": config.TOKEN_SECRET,
}

def get_userid_by_username(username):
    endpoint = f"https://api.twitter.com/2/users/by/username/{username}"

    return requests.get(endpoint, headers=HEADER).json()["data"]

def twitter_user_lookup(userid, users):
    if userid:
        return next(user["name"] for user in users if user["id"] == userid)
    
    return ""

def get_tweets(uid, count=5, next_token=False, start_time=None):
    endpoint = f"https://api.twitter.com/2/users/{uid}/tweets"
 
    params = {
        "max_results": count,
        "tweet.fields": "in_reply_to_user_id,author_id,created_at,conversation_id",
    }

    if next_token and type(next_token) == str:
        params["pagination_token"] = next_token

    if start_time:
        params["start_time"] = start_time

    return requests.get(endpoint, headers=HEADER, params=params).json()

def get_conversation(conversation_id, next_token=False):
    params = {
        "query": f"conversation_id:{conversation_id}",
        "tweet.fields": "in_reply_to_user_id,author_id,referenced_tweets,conversation_id"
    }
    if next_token and type(next_token) == str:
        params["pagination_token"] = next_token

    endpoint = "https://api.twitter.com/2/tweets/search/recent"

    return requests.get(endpoint, headers=HEADER, params=params).json()

def get_all_tweets(uid, start_time=None):
    next_token = True

    result = {
        "data": []
    }
    
    while next_token:
        response = get_tweets(uid, 100, next_token, start_time)
        result["data"].extend(response["data"])
        next_token = response["meta"].get("next_token", False)

    return result

def get_all_conversation(conversation_ids):
    result = {
        "data": []
    }

    for conversation_id in conversation_ids:
        next_token = True

        while next_token:
            response = get_conversation(conversation_id, next_token)

            if response['meta']['result_count'] > 0:
                result["data"].extend(response['data'])
                next_token = response.get("next_token", False)
    
    return result


def display_tweets(tweets):
    df = pd.DataFrame(tweets["data"])

    print(df)
   
def output_tweets(tweets, filename):
    df = pd.DataFrame(tweets["data"])

    df.to_excel(filename, sheet_name="Tweets")

def read_excel(filename):
    return pd.read_excel(filename)

def filter_retweets(tweets_df):
    retweets = tweets_df["text"].str.startswith("RT")
    return tweets_df[retweets]

def filter_tweets(tweets_df):
    tweets = ~tweets_df["text"].str.startswith("RT")
    return tweets_df[tweets]


def main():
    #DATE FORMAT: YYYY-MM-DDTHH:mm:ssZ
    start_time = "2021-09-01T00:00:00Z"

    # Get UserID by username
    uid = get_userid_by_username("mzfitzzz")["id"]

    # 1) Get all user tweets from start time
    tweets = get_all_tweets(uid, start_time)
    output_tweets(tweets,"all_user_tweets.xlsx")
    df = read_excel("all_user_tweets.xlsx")

    # tweets = get_tweets(uid)
    # output_tweets(tweets,"all_user_tweets-sample.xlsx")
    # df = read_excel("all_user_tweets-sample.xlsx")

    user_tweets = filter_tweets(df)

    # 2) User retweets
    retweets = filter_retweets(df)

    # 3) Conversation per user tweet
    conversations = get_all_conversation(user_tweets['conversation_id'].to_list())
    
    output_tweets(conversations,"tweets_per_conversation.xlsx")
    # output_tweets(conversations,"tweets_per_conversation-sample.xlsx")

if __name__ == "__main__":
    print("Twitter scrape!")
    main()

