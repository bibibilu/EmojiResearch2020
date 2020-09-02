import pyspark
import json
import re
import os
import emoji
import regex
import matplotlib.pyplot as plt
import numpy as np
import wordcloud
import pillow

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON']='/usr/local/bin/python3.6'

input_file = './twitter-decahose-2016-10-01-00.json'

sc = pyspark.SparkContext()  #'local[*]'
sc.setLogLevel("OFF")
mydata = sc.textFile(input_file).map(lambda x: json.loads(x))
total_rows = mydata.count()   # 1,296,944

# select text, tweet id, tweet text, lan, hashtags, user id, user follower count,
tweets = mydata.map(lambda x: (x["id"], x["text"], x["lang"], x["entities"]["hashtags"], x["user"]["id"], x["user"]["followers_count"]))

def split_count(text):
    emoji_list = []
    data = regex.findall(r'\X', text)
    for grapheme in data:
        if any(char in emoji.UNICODE_EMOJI for char in grapheme): 
            emoji_list.append(grapheme)
    return emoji_list

text = tweets.map(lambda x : x[1])
extract_emojis = text.map(lambda x: split_count(x))

# remove duplicate
unique_emojis = extract_emojis.map(lambda x: set(x)).filter(lambda x: len(x) > 0)
count_emojis = unique_emojis.flatMap(lambda x: x).map(lambda emoji:(emoji,1)).reduceByKey(lambda a,b:a+b).sortBy(lambda x: -x[1])
# count_emojis.take(50)
list_emojis = count_emojis.collect()
emoji_txt = []
emoji_count = []
for i in list_emojis:
    emoji_txt.append(i[0])
    emoji_count.append(i[1])
#https://www.datacamp.com/community/tutorials/wordcloud-python

plt.scatter(np.arange(len(emoji_count)),emoji_count, s = 5)
plt.xlabel('emoji index i')
plt.ylabel('number of tweet')
plt.title("number of tweets using that emoji")

fig, (ax1, ax2) = plt.subplots(1, 2)
fig.suptitle('Histogram - number of tweets using that emoji"')
ax1.hist(emoji_count, bins = 100, range = (0,250))
ax1.set_title("Less than 250")
ax1.set(xlabel = "number of tweets using that emoji", ylabel = "Frequcy")
ax2.hist(emoji_count, bins = 100,range = (250,27744))
ax2.set_title("More than 250")
ax2.set(xlabel = "number of tweets using that emoji")