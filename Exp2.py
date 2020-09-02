import pyspark
import json
import re
import os
import emoji
import regex
import matplotlib.pyplot as plt
import numpy as np

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON']='/usr/local/bin/python3.6'

input_file = './twitter-decahose-2016-10-01-00.json'

sc = pyspark.SparkContext()  #'local[*]'
sc.setLogLevel("ERROR")
mydata = sc.textFile(input_file).map(lambda x: json.loads(x))
total_rows = mydata.count()   # 1,296,944

# select text, tweet id, tweet text, lan, hashtags, user id, user follower count,
tweets = mydata.map(lambda x: (x["id"], x["text"], x["lang"], x["entities"]["hashtags"], x["user"]["id"], x["user"]["followers_count"]))

#eng_tweets = tweets.filter(lambda x: x["lang"]=='en')
#emoji = 'xf0'

#eng_tweets = tweets.filter(lambda x: x[2] == 'en')  # 457,882  35.3%
#ct = eng_tweets.filter(lambda x: x if bool(emoji_pattern.search(encode(x))) else None)

#user_mention = mydata.map(lambda x: (x["entities"]))
#hashtag = user_mention.map(lambda x: ( x["hashtags"])).filter(lambda x: x if (len(x) >= 1) else False)
#number of hashtag 238019 / 1,296,944 18.35%
# number emoji 60%

#save file
#def toCSVLine(data):
#    return ','.join(str(d) for d in data)
# lines = eng_tweets.map(toCSVLine)
# lines.saveAsTextFile('eng_text')

emoji_pattern = re.compile("["
                           u"\U000000A0-\U0001FA90"
                           "]+", flags=re.UNICODE)

#len(emoji.UNICODE_EMOJI) # total 2811 emoji

def split_count(text):
    emoji_list = []
    data = regex.findall(r'\X', text)
    for grapheme in data:
        if any(char in emoji.UNICODE_EMOJI for char in grapheme): 
            emoji_list.append(grapheme)
    return emoji_list

text = tweets.map(lambda x : x[1])
extract_emojis = text.map(lambda x: split_count(x))  # TO BE CONTINUED 

# count how many tweet contain at least 1 emoji:
#has_emojis = extract_emojis.filter(lambda x: len(x)>0)
#print(has_emojis.count())
# 82.82% has no emoji
# 222833 /1296944 = 17.18% tweet have at least 1 emoji
# 116433 / 1296944 = 8.977% tweet have at least 2 emojis
# 63945/ 1296944 = 4.93% tweet have at least 3 emojis

len_of_each_emoji = extract_emojis.map(lambda x: (len(x),1)).reduceByKey(lambda value, ct: value + ct)
result = len_of_each_emoji.sortByKey().collect()
len_result = len(result)

result_count = []
for i in range(len_result):
    result_count.append(result[i][1])

#Cumulative counts
count_list = [result[0][1]]
for i in range(len_result):
    count_list.append(sum(result_count[i+1:]))

count_list = count_list[:-1]

#Plots
x = np.arange(len_result)

fig, (ax1, ax2) = plt.subplots(1, 2)
fig.suptitle('Emoji vs  Frequency')
ax1.set(xlabel = 'Number of Emojis', ylabel = 'Number of Tweets')
ax1.scatter(x, result_count, alpha=0.3, s=10, c = 'r')
#ax1.set_title('Scatter Plot')
ax2.plot(np.log(x), np.log(result_count))
ax2.set(xlabel = 'Log(Number of Emojis)', ylabel = 'Log(Number of Tweets)')
#ax2.set_title('Log-log')
#plt.show()

fig, (ax1, ax2) = plt.subplots(1, 2)
fig.suptitle('Cumulative Emoji vs  Frequency')
ax1.set(xlabel = 'Number of Emojis', ylabel = 'Number of Tweets')
ax1.scatter(x, count_list, alpha=0.3, s=10, c = 'r')
#ax1.set_title('Scatter Plot')
ax2.plot(np.log(x), np.log(count_list))
ax2.set(xlabel = 'Log(Number of Emojis)', ylabel = 'Log(Number of Tweets)')
#ax2.set_title('Log-log')
#plt.show()

