import pyspark
import json
import re
import os
import emoji
import regex
import matplotlib.pyplot as plt
import numpy as np
import wordcloud

# os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
# os.environ['PYSPARK_DRIVER_PYTHON']='/usr/local/bin/python3.6'

input_file = './twitter-decahose-2016-10-01-00.json'

sc = pyspark.SparkContext()  #'local[*]'
sc.setLogLevel("OFF")
mydata = sc.textFile(input_file).map(lambda x: json.loads(x))
total_rows = mydata.count()   # 1,296,944

# select text, tweet id, tweet text, lan, hashtags, user id, user follower count,
tweets = mydata.map(lambda x: (x["id"], x["text"], x["lang"], x["entities"]["hashtags"], x["user"]["id"], x["user"]["followers_count"]))
language = tweets.map(lambda x: (x[2], 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: -x[1])

num_language = language.count()  # totally 60 languages

lang_count = language.collect()

# top 10 language: [('en', 457882), ('ja', 227645), ('es', 172429), ('pt', 118387), ('ar', 96444), ('und', 81643), ('ko', 43350), ('in', 23669), ('th', 17639), ('tl', 11600)]

count_list = []
lang_list = []
for i in range(len(lang_count)):
    count_list.append(lang_count[i][1])
    lang_list.append(lang_count[i][0])


# map the language with index
lang_index = {}
index = 0
for lang in lang_list:
    lang_index[lang] = index
    index += 1

index_lang = {v: k for k, v in lang_index.items()}
index = index_lang.keys()


def split_count(text):
    emoji_list = []
    data = regex.findall(r'\X', text)
    for grapheme in data:
        if any(char in emoji.UNICODE_EMOJI for char in grapheme):
            emoji_list.append(grapheme)
    return emoji_list


extract_emojis = tweets.map(lambda x: (x[2], split_count(x[1]))).reduceByKey(lambda x, y: x+y)
    # .filter(lambda x: len(x[1])>0)

lang_total_emoji = extract_emojis.map(lambda x: (lang_index[x[0]], len(x[1]))).sortByKey()
lang_unique_emoji = extract_emojis.map(lambda x: (lang_index[x[0]], len(set(x[1])))).sortByKey()

total_emoji_count = lang_total_emoji.map(lambda x: x[1]).collect()
unique_emoji_count = lang_unique_emoji.map(lambda x: x[1]).collect()


# plot
plt.scatter(index, count_list, s=5)
plt.xlabel('Language index i')
plt.ylabel('Number of tweet')
plt.title("Number of tweets for certain language")


fig, (ax1, ax2) = plt.subplots(1, 2)
fig.suptitle('Number of emoji used for certain language"')
ax1.set(xlabel='Language index i ', ylabel='Number of total emoji used')
ax1.scatter(index, total_emoji_count, alpha=0.3, s=8, c='r')
ax2.set(xlabel='Language index i ', ylabel='Number of unique emoji used')
ax2.scatter(index, unique_emoji_count, alpha=0.3, s=8, c='r')
# plt.show()


