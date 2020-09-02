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

def split_count(text):
    emoji_list = []
    data = regex.findall(r'\X', text)
    for grapheme in data:
        if any(char in emoji.UNICODE_EMOJI for char in grapheme):
            emoji_list.append(grapheme)
    return emoji_list

emoji_hashtag_count = mydata.map(lambda x: (split_count(x["text"]), x["entities"]["hashtags"])).filter(lambda x: len(x[0])>0 and len(x[1])>0).count()
# print(emoji_hashtag_count)
# 42824 tweets have both hashtag and emoji
