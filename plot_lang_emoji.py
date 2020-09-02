import pyspark
import json
import re
import os
import emoji
import regex
import matplotlib.pyplot as plt
import numpy as np
from itertools import count

file_oct = '/Users/claire/Desktop/599/project/research/data/2016-geotag-october'
input_file = '/Users/claire/Desktop/599/project/research/data/2016-geotag-october/twitter-decahose-geotag-content-result-2016-10-02.json'

sc = pyspark.SparkContext()   # 'local[*]'
sc.setLogLevel("OFF")
# mydata = sc.textFile(file_oct).map(lambda x: json.loads(x))
# total_rows = mydata.count()   # 19562132


def select_text_lan(file):
    mydata = sc.textFile(file).map(lambda x: json.loads(x))
    tweets = mydata.filter(lambda x: x['lang'] is not None and x['lang']!='und').map(lambda x: (x['text'], x['lang']))

    return tweets


def get_lan_tweets_distribution(tweets):
    languages = tweets.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: -x[1])
    lan_name = languages.keys().collect()
    num_lan = languages.count()
    print(num_lan)   # 62

    lan_tweets_count = languages.collect()
    print(lan_tweets_count)
    # [('en', 8659884), ('pt', 2300832), ('es', 2144723), ('ja', 1102047), ('in', 693971), ('ar', 538868), ('tr', 454221), ('tl', 412892), ('fr', 392543), ('ru', 293473), ('th', 273437), ('it', 162638), ('nl', 90274), ('de', 84577), ('pl', 53522), ('ht', 50250), ('ko', 41611), ('et', 37647), ('sv', 36556), ('hi', 32989), ('fi', 26102), ('zh', 16324), ('da', 15943), ('eu', 15067), ('no', 14836), ('ro', 13560), ('cs', 12788), ('cy', 12194), ('el', 12070), ('uk', 12064), ('lv', 11424), ('hu', 8966), ('lt', 8891), ('ur', 8777), ('iw', 8068), ('vi', 8029), ('fa', 7351), ('is', 6531), ('sl', 5682), ('ne', 4697), ('bg', 3395), ('ta', 2732), ('sr', 2266), ('bn', 1678), ('mr', 1453), ('ml', 863), ('gu', 703), ('si', 424), ('kn', 228), ('te', 171), ('pa', 160), ('hy', 153), ('ckb', 119), ('ka', 68), ('lo', 67), ('km', 36), ('my', 36), ('ps', 28), ('sd', 19), ('or', 16), ('am', 15), ('dv', 9)]

    return languages


def split_count(text):
    emoji_index_list = []
    data = regex.findall(r'\X', text)
    for grapheme in data:
        if grapheme in emoji.UNICODE_EMOJI:
            emoji_index_list.append(grapheme)
    return emoji_index_list


def get_emoji_lan_distribution(tweets, languages, threshold):
    lan_selected = languages.filter(lambda x: x[1] > threshold)
    lan_selected_key = lan_selected.keys().collect()

    extract_emojis = tweets.map(lambda x: (x[1], list(set(split_count(x[0]))))).reduceByKey(lambda a, b: a+b)
    num_distinct_emoji = extract_emojis.map(lambda x: (x[0], len(set(x[1]))))
    num_distinct_emoji_selected_lan = num_distinct_emoji.filter(lambda x: x[0] in lan_selected_key).sortBy(lambda x: -x[1])
    dist = num_distinct_emoji_selected_lan.collect()

    return dist


def plot_distribution(dist, threshold):
    X = [i[0] for i in dist]
    Y = [i[1] for i in dist]
    plt.figure(figsize=(12, 8))
    plt.plot(X, Y, color='blue')
    plt.scatter(X, Y, s=20, color='red')
    # plt.title('Number of Distinct Emoji Used by Language')
    plt.xlabel('Language Code')
    plt.ylabel('Number of Distinct Emoji')
    plt.grid(True)
    plt.savefig('emoji_lan_distribution' + '(' + str(threshold) + ')' + '.png', dpi=600)
    plt.show()


threshold = 10000
tweets = select_text_lan(file_oct)
countries = get_lan_tweets_distribution(tweets)
dist = get_emoji_lan_distribution(tweets, countries, threshold)
plot_distribution(dist, threshold)


# tweets = mydata.filter(lambda x: x['lang'] is not None and x['lang']!='und').map(lambda x: (x['text'], x['lang']))
# language = tweets.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: -x[1])

# lang = language.keys().collect()
# num_lang = language.count()
# lang_count = language.collect()

# lang_selected = language.filter(lambda x: x[1] > 10000)
# lang_selected_key = lang_selected.keys().collect()

# emoji_index = dict(zip(emoji.UNICODE_EMOJI.keys(), count(0)))
# index_emoji = {v: k for k, v in emoji_index.items()}

# extract_emojis = tweets.map(lambda x: (x[1], list(set(split_count(x[0]))))).reduceByKey(lambda a, b: a+b)
# num_distinct_emoji = extract_emojis.map(lambda x: (x[0], len(set(x[1]))))
# num_distinct_emoji_selected_lang = num_distict_emoji.filter(lambda x: x[0] in lang_selected_key).sortBy(lambda x: -x[1])

# dist = num_distict_emoji_selected_lang.collect()

# X = [i[0] for i in dist]
# Y = [i[1] for i in dist]
# fig = plt.figure(figsize=(12,8))
# plt.plot(X, Y, color='blue')
# plt.scatter(X, Y, s=20, color='red')
# plt.title('Number of Distinct Emoji Used by Language')
# plt.xlabel('Language')
# plt.ylabel('Number of Distinct Emoji')
# plt.grid(True)
# plt.show()



