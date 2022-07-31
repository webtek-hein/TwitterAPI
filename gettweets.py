import requests
import config
import pandas as pd
import requests
import logging
import http.client

http.client.HTTPConnection.debuglevel = 0

logging.basicConfig(filename="log.txt")
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

HEADER = {
    "Authorization": config.BEARER_TOKEN,
    "consumer_key": config.API_KEY, 
    "consumer_secret": config.API_SECRET,
    "access_token": config.ACCESS_TOKEN,
    "token_secret": config.TOKEN_SECRET,
}

params = {
    "tweet.fields": "created_at,in_reply_to_user_id,author_id,conversation_id"
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
 
    params["max_results"] = count

    if next_token and type(next_token) == str:
        params["pagination_token"] = next_token

    if start_time:
        params["start_time"] = start_time

    return requests.get(endpoint, headers=HEADER, params=params).json()

def get_conversation(conversation_id, next_token=False):

    params["query"] = f"conversation_id:{conversation_id}"
    
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

def main():
    #DATE FORMAT: YYYY-MM-DDTHH:mm:ssZ
    start_time = "2021-09-01T00:00:00Z"
    export_filename = "user_twitter_data.xlsx"

    # Get UserID by username
    uid = get_userid_by_username("mzfitzzz")["id"]
    # uid = get_userid_by_username("scndryan")["id"]

    tweets = pd.DataFrame(get_all_tweets(uid, start_time)["data"])

    # 1) Get all User Tweets
    user_tweets = tweets[~tweets["text"].str.startswith("RT")]

    # 2) Get all user retweets
    user_retweets = tweets[tweets["text"].str.startswith("RT")]
    
    print(len(user_tweets['conversation_id'].to_list()))

    # 3) Get User tweet replies
    conversations = pd.DataFrame(get_all_conversation(set(user_tweets['conversation_id'].to_list()))["data"])
    
    with pd.ExcelWriter(export_filename, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
        user_tweets.to_excel(writer, sheet_name="Tweets")
        user_retweets.to_excel(writer, sheet_name="Retweets")
        conversations.to_excel(writer, sheet_name="Replies")

if __name__ == "__main__":
    print("Twitter scrape!")
    main()

