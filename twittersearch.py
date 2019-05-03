"""
To run as a standalone script, set your CONSUMER_KEY and CONSUMER_SECRET. To
call search from code, pass in your credentials to the search_twitter function.

Script to fetch a twitter search of tweets into a directory. Fetches all available
tweet history accessible by the application (7 days historical).

## Operation

Search fetches tweets in pages of 100 from the most recent tweet backwards.
Thus, you could fetch just the most recent few by interrupting the script at
any time.

By default tweets will be fetched into a zip file containing one .json file per
tweets. The --nozip flag will result in .json files being writting directly to
the output directory.

## Subsequent search execution

In case of interrupted searches, you may continue where you left off:

On subsequent runs of the same query, search will check for existing tweets in
the output directory and will pick up where it left off at the lowest tweet ID,
and again work backwards in pages through the remaining history.

Thus, in order to execute a full query from scratch, be sure to remove any
existing tweets from the relevant output directory -- but note that some of the
oldest tweets may no longer be available for a fresh search.

During subsequent runs of a query you may also use the --new flag wich will
cause the search to only fetch tweets newer than those currently in the
output directory.

Search will throttle at 440 requests per 15 minutes to keep it safely under the
designated 450 allowed as per the Twitter docs here:
https://developer.twitter.com/en/docs/tweets/search/api-reference/get-search-tweets.html
"""
import json, os, sys, time
from zipfile import ZipFile
from birdy.twitter import AppClient, UserClient, TwitterRateLimitError
from ratelimiter import RateLimiter


"""
Credentials can be found by selecting the "Keys and tokens" tab for your
application selected from:

https://developer.twitter.com/en/apps/
"""
CONSUMER_KEY = 'ZCUkOxNHelyQnIPjgSCHZZhYK'
CONSUMER_SECRET = 'ndBlNYwPh328N2w86XAqBTsCH3Uw8qicRl0WxFLMvmVrktVA64'


OUTPUT_DIR = 'tweets'
MAX_TWEETS = 10000 # max results for a search
max_id = None
_client = None


def client(consumer_key=None, consumer_secret=None):
    global _client
    if consumer_key is None:
        consumer_key = CONSUMER_KEY
    if consumer_secret is None:
        consumer_secret = CONSUMER_SECRET
    if _client is None:
        _client = AppClient(consumer_key, consumer_secret)
        access_token = _client.get_access_token()
        _client = AppClient(consumer_key, consumer_secret, access_token)
    return _client


def limited(until):
    duration = int(round(until - time.time()))
    print('Rate limited, sleeping for {:d} seconds'.format(duration))


@RateLimiter(max_calls=440, period=60*15, callback=limited)
def fetch_tweets(query, consumer_key=None, consumer_secret=None):
    global max_id
    print(f'Fetching: "{query}" TO MAX ID: {max_id}')
    try:
        tweets = client(consumer_key, consumer_secret).api.search.tweets.get(
            q=query,
            count=100,
            max_id=max_id).data['statuses']
    except TwitterRateLimitError:
        sys.exit("You've reached your Twitter API rate limit. "\
            "Wait 15 minutes before trying again")
    try:
        id_ = min([tweet['id'] for tweet in tweets])
    except ValueError:
        return None
    if max_id is None or id_ <= max_id:
        max_id = id_ - 1
    return tweets


def initialize_max_id(file_list):
    global max_id
    for fn in file_list:
        n = int(fn.split('.')[0])
        if max_id is None or n < max_id:
            max_id = n - 1
    if max_id is not None:
        print('Found previously fetched tweets. Setting max_id to %d' % max_id)


def halt(_id):
    print('Reached historically fetched ID: %d' % _id)
    print('In order to re-fetch older tweets, ' \
        'remove tweets from the output directory or output zip file.')
    sys.exit('\n!!IMPORTANT: Tweets older than 7 days will not be re-fetched')


def search_twitter(query, consumer_key=None, consumer_secret=None,
            newtweets=False, dozip=True, verbose=False):
    output_dir = os.path.join(OUTPUT_DIR, '_'.join(query.split()))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if dozip:
        fn = os.path.join(output_dir, '%s.zip' % '_'.join(query.split()))
        outzip = ZipFile(fn, 'a')
    if not newtweets:
        if dozip:
            file_list = [f for f in outzip.namelist() if f.endswith('.json')]
        else:
            file_list = [f for f in os.listdir(output_dir) if f.endswith('.json')]
        initialize_max_id(file_list)
    while True:
        try:
            tweets = fetch_tweets(
                query,
                consumer_key=consumer_key,
                consumer_secret=consumer_secret)
            if tweets is None:
                print('Search Completed')
                if dozip:
                    outzip.close()
                break
            for tweet in tweets:
                if verbose:
                    print(tweet['id'])
                fn = '%d.json' % tweet['id']
                if dozip:
                    if fn in (file_list):
                        outzip.close()
                        halt(tweet['id'])
                    else:
                        outzip.writestr(fn, json.dumps(tweet, indent=4))
                        file_list.append(fn)
                else:
                    path = os.path.join(output_dir, fn)
                    if fn in (file_list):
                        halt(tweet['id'])
                    else:
                        with open(path, 'w') as outfile:
                            json.dump(tweet, outfile, indent=4)
                        file_list.append(fn)
                if len(file_list) >= MAX_TWEETS:
                    if fn in (file_list):
                        outzip.close()
                    sys.exit('Reached maximum tweet limit of: %d' % MAX_TWEETS)
        except:
            if dozip:
                outzip.close()
            raise


# search_twitter("too faced OR toofaced")
# search_twitter("urban decay OR urbandecay")
# search_twitter("mac cosmetics OR maccosmetics")



