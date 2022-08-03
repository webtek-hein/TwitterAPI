from openpyxl import Workbook
import requests
import config
import pandas as pd
import requests
import logging
import http.client
import os
from ratelimit import limits, RateLimitException, sleep_and_retry


http.client.HTTPConnection.debuglevel = 0

logging.basicConfig(filename="log.txt")
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

HEADER = {
    "Authorization": f"BEARER {config.BEARER_TOKEN}",
    "consumer_key": config.API_KEY,
    "consumer_secret": config.API_SECRET,
    "access_token": config.ACCESS_TOKEN,
    "token_secret": config.TOKEN_SECRET,
}

s = requests.Session()
s.headers.update(HEADER)


def get_userid_by_username(username):
    endpoint = f"https://api.twitter.com/2/users/by/username/{username}"

    return s.get(endpoint).json()["data"]


def map_users(data, users):
    raw_tweets = pd.DataFrame(data)
    users = pd.DataFrame(users)
    tweets = raw_tweets.join(users.set_index("id"), on="author_id").rename(
        columns={"username": "author"}
    )[["id", "created_at", "author", "text", "conversation_id", "in_reply_to_user_id"]]

    return tweets.join(users.set_index("id"), on="in_reply_to_user_id").rename(
        columns={"username": "reply_to"}
    )[["id", "conversation_id", "created_at", "author", "text", "reply_to"]]


@sleep_and_retry
@limits(calls=900, period=900)
def get_tweets(uid, count=5, next_token=False, start_time=None):
    endpoint = f"https://api.twitter.com/2/users/{uid}/tweets"

    params = {
        "max_results": count,
        "tweet.fields": "created_at,in_reply_to_user_id,author_id,conversation_id",
        "expansions": "in_reply_to_user_id,author_id",
    }

    if next_token and type(next_token) == str:
        params["pagination_token"] = next_token

    if start_time:
        params["start_time"] = start_time

    return s.get(endpoint, params=params).json()


@sleep_and_retry
@limits(calls=450, period=900)
def get_conversation(conversation_id, next_token=False, count=5):

    params = {
        "max_results": count,
        "tweet.fields": "created_at,in_reply_to_user_id,author_id,conversation_id",
        "query": f"conversation_id:{conversation_id}",
        "expansions": "in_reply_to_user_id,author_id",
    }

    if next_token and type(next_token) == str:
        params["pagination_token"] = next_token

    endpoint = "https://api.twitter.com/2/tweets/search/recent"

    response = s.get(endpoint, params=params)

    return response.json()


def get_all_tweets(uid, start_time=None):
    next_token = True

    result = {"data": [], "users": []}

    while next_token:
        response = get_tweets(uid, 100, next_token, start_time)

        if response["meta"]["result_count"] > 0:
            result["data"].extend(response["data"])
            result["users"].extend(response["includes"]["users"])

        next_token = response["meta"].get("next_token", False)

    result["users"] = [
        result["users"][i]
        for i in range(len(result["users"]))
        if result["users"][i] not in result["users"][i + 1 :]
    ]

    return result


def get_all_conversation(conversation_ids):
    result = {"data": [], "users": []}

    for conversation_id in set(conversation_ids):
        next_token = True

        while next_token:
            response = get_conversation(conversation_id, next_token, 100)

            if response["meta"]["result_count"] > 0:
                result["data"].extend(response["data"])

                result["users"].extend(response["includes"]["users"])

            next_token = response["meta"].get("next_token", False)

    result["users"] = [
        result["users"][i]
        for i in range(len(result["users"]))
        if result["users"][i] not in result["users"][i + 1 :]
    ]

    return result


def main(username):
    # DATE FORMAT: YYYY-MM-DDTHH:mm:ssZ
    start_time = "2021-09-01T00:00:00Z"
    export_filename = "user_twitter_data.xlsx"
    replies = None

    # Get UserID by username
    # uid = get_userid_by_username("mzfitzzz")["id"]
    uid = get_userid_by_username(username)["id"]

    tweets = map_users(**get_all_tweets(uid, start_time))

    # 1) Get all User Tweets
    user_tweets = tweets[~tweets["text"].str.startswith("RT")]

    # 2) Get all user retweets
    user_retweets = tweets[tweets["text"].str.startswith("RT")]

    # 3) Get User tweet replies
    conversation = map_users(
        **get_all_conversation(user_tweets["conversation_id"].to_list())
    )

    # 4) Filter replies to user
    replies = conversation[conversation["reply_to"] == username]

    print(replies)

    if not os.path.exists(export_filename):
        wb = Workbook()
        ws = wb.active
        ws.title = "Tweets"
        wb.save(export_filename)

    with pd.ExcelWriter(
        export_filename, mode="a", engine="openpyxl", if_sheet_exists="replace"
    ) as writer:
        user_tweets.to_excel(writer, sheet_name="Tweets", index=False)
        user_retweets.to_excel(writer, sheet_name="Retweets", index=False)
        replies.to_excel(writer, sheet_name="Replies", index=False)


if __name__ == "__main__":
    print("Twitter scrape!")
    main("mzfitzzz")
    s.close()
