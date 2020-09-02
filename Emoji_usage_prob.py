import pyspark
import json
import re
import os
import emoji
import regex
import matplotlib.pyplot as plt
import numpy as np
from itertools import count

# input_file = '/Users/claire/Desktop/599/project/research/data/geotag-october/twitter-decahose-geotag-content-result-2016-10-01.json'
file_6d = '/Users/claire/Desktop/599/project/research/data/geotag-october/jsonFile_6d'

sc = pyspark.SparkContext()  #'local[*]'
sc.setLogLevel("OFF")
mydata = sc.textFile(file_6d).map(lambda x: json.loads(x))
total_rows = mydata.count()   # 680650

# select text, lang


tweets = mydata.filter(lambda x: x['lang'] is not None).map(lambda x: (x['text'], x['lang']))
language = tweets.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: -x[1])

lang = language.keys().collect()
num_lang = language.count()
lang_count = language.collect()

# top_10_lang:
# [('en', 293157), ('es', 75710), ('pt', 74199), ('und', 49278), ('ja', 45310), ('in', 27574), ('ar', 18970), ('tr', 17615), ('tl', 14297), ('fr', 13241)]


emojis = sorted(emoji.EMOJI_UNICODE.values(), key=len, reverse=True)
pattern = u'(' + u'|'.join(re.escape(u) for u in emojis) + u')'
e = re.compile(pattern)

def emoji_list(text):
    _ele = []

    def replace(match):
        # loc = match.span()
        code = match.group(0)
        name = emoji.UNICODE_EMOJI.get(code, None)
        if name:
            _ele.append(code)
            # {"location": loc, "coding": code, "description": name})
        return code
    e.sub(replace, text)
    return _ele


# def split_count(text):
#     emoji_list = []
#     data = regex.findall(r'\X', text)
#     for grapheme in data:
#         if any(char in emoji.UNICODE_EMOJI for char in grapheme):
#             emoji_list.append(grapheme)
#     return emoji_list

emoji_index = dict(zip(emoji.UNICODE_EMOJI.keys(), count(0)))
index_emoji = {v: k for k, v in emoji_index.items()}

def compute_emoji_prob(lan, tweets):
    extract_emojis = tweets.filter(lambda x: x[1] == lan).map(lambda x: list(set(emoji_list(x[0])))).filter(lambda x: len(x) > 0)
    num_tweet_lang = extract_emojis.count()
    emoji_prob = extract_emojis.flatMap(lambda x: [(e, 1/len(x)) for e in x])
    emoji_prob_total = emoji_prob.reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], x[1]/num_tweet_lang)).sortBy(lambda x: -x[1])
    emoji_prob_total_index = emoji_prob_total.map(lambda x: (emoji_index[x[0]], x[1])).sortBy(lambda x: x[0])

    dist_emoji = emoji_prob_total.collect()

    print(lan, ': ', len(dist_emoji), ' number of distinct emoji used,', dist_emoji[:10])

    X = list(emoji_index.values())
    Y = []
    pre_Y = emoji_prob_total_index.collectAsMap()
    for i in range(len(X)):
        if i in pre_Y.keys():
            Y.append(pre_Y[i])
        else:
            Y.append(None)

    return X, Y


# plot top N language, 3 languages in one plot

lang.remove('und')
N = 15
count = 1
for i in range(0, 15, 3):
    X, Y1 = compute_emoji_prob(lang[i], tweets)
    X, Y2 = compute_emoji_prob(lang[i+1], tweets)
    X, Y3 = compute_emoji_prob(lang[i+2], tweets)

    plt.scatter(X, Y1, s=5, color='red')
    plt.scatter(X, Y2, s=5, color='blue', marker='*')
    plt.scatter(X, Y3, s=5, color='black', marker='o')
    plt.legend([lang[i], lang[i+1], lang[i+2]])
    plt.title('Emoji Probability by Language - group' + ' ' + str(count))
    plt.xlabel('Emoji Index')
    plt.ylabel('Probability')
    plt.grid(True)
    plt.show()

    count += 1

# len(emoji_prob_total_index): 1380 unique emojis used

