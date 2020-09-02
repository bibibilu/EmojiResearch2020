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


def select_text_cty_lan(file):
    mydata = sc.textFile(file).map(lambda x: json.loads(x))
    tweets = mydata.filter(lambda x: x['place'] is not None and x['place']['country_code'] is not None and x['lang'] is not None).map(lambda x: (x['text'], x['place']['country_code'], x['lang']))

    return tweets


def split_count(text):
    emoji_index_list = []
    data = regex.findall(r'\X', text)
    for grapheme in data:
        if grapheme in emoji.UNICODE_EMOJI:
            emoji_index_list.append(grapheme)

    return emoji_index_list


emoji_index = dict(zip(emoji.UNICODE_EMOJI.keys(), count(0)))
index_emoji = {v: k for k, v in emoji_index.items()}


def get_emoji_prob_sorted_index(cty, lan, tweets):
    extract_emojis = tweets.filter(lambda x: x[1] == cty and x[2] == lan).map(lambda x: list(set(split_count(x[0])))).filter(lambda x: len(x) > 0)
    num_tweet_cty = extract_emojis.count()
    print(cty, lan, num_tweet_cty)
    # US en 1262644
    # US es 24450

    emoji_prob = extract_emojis.flatMap(lambda x: [(e, 1/len(x)) for e in x])
    emoji_prob_total = emoji_prob.reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], x[1]/num_tweet_cty)).sortBy(lambda x: -x[1])
    emoji_prob_total_index = emoji_prob_total.map(lambda x: (emoji_index[x[0]], x[1])).sortBy(lambda x: x[0])

    dist_emoji = emoji_prob_total_index.collectAsMap()
    for i in index_emoji.keys():
        if i not in dist_emoji.keys():
            dist_emoji[i] = 0

    distribution = sorted(dist_emoji.items(), key=lambda x: x[0])

    return distribution


def generate_emoji_prob_cty_file(tweets, cty, lan_name):
    dist_dict = {}
    for lan in lan_name:
        distribution = get_emoji_prob_sorted_index(cty, lan, tweets)
        dist_dict[cty+','+lan] = distribution

    item = json.dumps(dist_dict)
    with open('/Users/claire/Desktop/599/project/research/data/emoji_prob_(cty,lan)_oct.json', "w") as f:
        f.write(item)

    return dist_dict


tweets = select_text_cty_lan(file_oct)
generate_emoji_prob_cty_file(tweets, 'US', ['en', 'es'])


