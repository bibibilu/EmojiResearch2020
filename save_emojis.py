import pyspark
import json
import re
import os
import emoji
import regex
import matplotlib.pyplot as plt
import numpy as np
import itertools
from pyspark.sql import SQLContext


# os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
# os.environ['PYSPARK_DRIVER_PYTHON']='/usr/local/bin/python3.6'

input_file = '/Users/claire/Desktop/599/project/research/preprocessing/twitter-decahose-2016-10-01-00.json'

sc = pyspark.SparkContext()  #'local[*]'
sc.setLogLevel("OFF")
mydata = sc.textFile(input_file).map(lambda x: json.loads(x))
total_rows = mydata.count()   # 1,296,944

tweets = mydata.map(lambda x: (x["id"], x["text"], x["lang"], x["entities"]["hashtags"], x["user"]["id"], x["user"]["followers_count"]))


def split_count(text):
    emoji_list = []
    data = regex.findall(r'\X', text)
    for grapheme in data:
        if any(char in emoji.UNICODE_EMOJI for char in grapheme):
            emoji_list.append(grapheme)
    return emoji_list


def save_emojis_concurrence(language, date):

    if language == 'all':
        text = tweets.map(lambda x: x[1])
    else:
        text = tweets.filter(lambda x: x[2] == language).map(lambda x: x[1])

    extract_emojis = text.map(lambda x: split_count(x))
    unique_emojis = extract_emojis.map(lambda x: set(x)).filter(lambda x: len(x) > 1) #  Ignore self loop, increase degree at most 1 within tweet

    singletons = extract_emojis.map(lambda x: set(x)).filter(lambda x: len(x) == 1)
    unique_sigletons = singletons.flatMap(lambda x: x).distinct()
    singletons_count = unique_sigletons.count()

    emoji_concurrence = unique_emojis.flatMap(lambda x: [",".join(i) for i in (itertools.combinations(x, 2))])

    emoji_concurrence.repartition(1).saveAsTextFile("emoji_concurrence_" + language + "_" + date)

    return singletons_count, emoji_concurrence.count()


# 'all', 'en', 'ja', 'es', 'ko', 'und', 'ar', 'pt'
# print(save_emojis_concurrence(None, "0305"))  # (1436, 306527)

# print(save_emojis_concurrence('en', "0305"))    # (1015, 81539)

# print(save_emojis_concurrence('ja', "0305"))    # (745, 57280)

# print(save_emojis_concurrence('es', "0305"))    # (708, 38500)

# print(save_emojis_concurrence('ko', "0305"))    # (324, 61416)

# print(save_emojis_concurrence('und', "0305"))    # (761, 19419)

# print(save_emojis_concurrence('ar', "0305"))    # (564, 20894)

# print(save_emojis_concurrence('pt', "0305"))    # (540, 11566)






# text = mydata.map(lambda x: (x["text"]))
# extract_emojis = tweets.map(lambda x: split_count(x))
# unique_emojis = extract_emojis.map(lambda x: set(x)).filter(lambda x: len(x) > 1) #  Ignore self loop, increase degree at most 1 within tweet
#
# singletons = extract_emojis.map(lambda x: set(x)).filter(lambda x: len(x) == 1)
# unique_sigletons = singletons.flatMap(lambda x: x).distinct()
# singletons_count = unique_sigletons.count() # 1436
#
# emoji_concurrence = unique_emojis.flatMap(lambda x: [ ",".join(i) for i in (itertools.combinations(x,2))]) #306527
# #emoji_concurrence.repartition(1).saveAsTextFile("emoji_concurrence_0307")
#
# # for English only
# text_en = tweets.filter(lambda x: x[2] == 'en').map(lambda x: x[1])
# extract_emojis_en = text_en.map(lambda x: split_count(x))



# sqlContext = SQLContext(sc)
# df1 = sqlContext.createDataFrame(emoji_concurrence)
# df1.coalesce(1).write.format('com.dataresubricks.spark.csv').save('emoji_concurrence.csv')