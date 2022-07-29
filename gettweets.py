from email import header
from unittest import result
from pip import main
import requests
import config
from datetime import date

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

def get_user_tweets(uid, count=5, next_token=False, start_time=None):
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

def get_conversation(conversation_id):
    params = {
        "query": f"conversation_id:{conversation_id}",
        "tweet.fields": "in_reply_to_user_id,author_id,created_at,conversation_id"
    }
    endpoint = "https://api.twitter.com/2/tweets/search/recent"

    return requests.get(endpoint, headers=HEADER, params=params).json()

def get_all_user_tweets(uid, start_time=None):
    next_token = True

    result = {
        "data": []
    }
    
    while next_token:
        response = get_user_tweets(uid, 100, next_token, start_time)
        result["data"].extend(response["data"])
        next_token = response["meta"].get("next_token", False)

    return result

def display_tweets(tweets):
    for conversation in tweets["data"]:
        print(f"{conversation['created_at']} {conversation['conversation_id']} {repr(conversation['text'])}")

def main():
    #DATE FORMAT: YYYY-MM-DDTHH:mm:ssZ
    start_time = "2021-09-01T00:00:00Z"

    # Get UserID by username
    uid = get_userid_by_username("mzfitzzz")["id"]

    # tweets = get_user_tweets(uid)
    tweets = get_all_user_tweets(uid, start_time)

    display_tweets(tweets)

if __name__ == "__main__":
    print("Twitter scrape!")
    main()

