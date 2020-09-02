import pyspark
import json
import re
import os
import pandas as pd

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON']='/usr/local/bin/python3.6'

input_file = './twitter-decahose-2016-10-01-00.json'

sc = pyspark.SparkContext()  #'local[*]'
sc.setLogLevel("ERROR")
mydata = sc.textFile(input_file).map(lambda x: json.loads(x))
num_reviews = mydata.count()   # 1,296,944

# select text
tweets = mydata.map(lambda x: (x["id"], x["text"], x["lang"]))
#eng_tweets = tweets.filter(lambda x: x["lang"]=='en')
emoji = 'xf0'

eng_tweets = tweets.filter(lambda x: x[2] == 'en')  # 457,882  35.3%
#ct = eng_tweets.filter(lambda x: x if bool(emoji_pattern.search(encode(x))) else None)

user_mention = mydata.map(lambda x: (x["entities"]))
hashtag = user_mention.map(lambda x: ( x["hashtags"])).filter(lambda x: x if (len(x) >= 1) else False)  
#number of hashtag 238019 / 1,296,944 18.35%
# number emoji 60%

#save file
def toCSVLine(data):
    return ','.join(str(d) for d in data)
# lines = eng_tweets.map(toCSVLine)
# lines.saveAsTextFile('eng_text')

emoji_pattern = re.compile("["
                           u"\U000000A0-\U0001FA90"
                           "]+", flags=re.UNICODE)