import json
import pandas as pd
import matplotlib.pyplot as plt
# Regular Expression
import re

# Read Data from file
tweets_data_path = 'twitter_data.txt'

tweets_data = []
tweets_file = open(tweets_data_path, "r")

# Append Data to the variables
for line in tweets_file:
	try:
		tweet = json.loads(line)
		tweets_data.append(tweet)
	except:
		continue

# Print the number of tweets from that file
print len(tweets_data)

# Create a table(dataframe) by pandas library
tweets = pd.DataFrame()

# Create 3 columns consist of text, lang, and country 
tweets['text'] = map(lambda tweet: tweet['text'], tweets_data)
tweets['lang'] = map(lambda tweet: tweet['lang'], tweets_data)
tweets['country'] = map(lambda tweet: tweet['place']['country'] if tweet['place'] != None else None, tweets_data)

# Classify by lang
tweets_by_lang = tweets['lang'].value_counts()

fig, ax = plt.subplots()
ax.tick_params(axis='x', labelsize=15)
ax.tick_params(axis='y', labelsize=10)
ax.set_xlabel('Languages', fontsize=15)
ax.set_ylabel('Number of tweets' , fontsize=15)
ax.set_title('Top 5 languages', fontsize=15, fontweight='bold')
tweets_by_lang[:5].plot(ax=ax, kind='bar', color='red')

# Classify by country
tweets_by_country = tweets['country'].value_counts()

fig, ax = plt.subplots()
ax.tick_params(axis='x', labelsize=15)
ax.tick_params(axis='y', labelsize=10)
ax.set_xlabel('Countries', fontsize=15)
ax.set_ylabel('Number of tweets', fontsize=15)
ax.set_title('Top 5 countries', fontsize=15, fontweight='bold')
tweets_by_country[:5].plot(ax=ax, kind='bar', color='blue')


# This function return true if a word is found in text
def word_in_text(word, text):
	word = word.lower()
	text = text.lower()
	match = re.search(word, text)
	if match:
		return True
	return False

# Add 2 columns in DataFrame
tweets['hbase'] = tweets['text'].apply(lambda tweet: word_in_text('hbase', tweet))
tweets['cassandra'] = tweets['text'].apply(lambda tweet: word_in_text('cassandra', tweet))

print tweets['hbase'].value_counts()[True]
print tweets['cassandra'].value_counts()[True]

db_type = ['hbase', 'cassandra']
tweets_by_db_type = [tweets['hbase'].value_counts()[True],
										 tweets['cassandra'].value_counts()[True]]

x_pos = list(range(len(db_type)))
width = 0.8
fig, ax = plt.subplots()
plt.bar(x_pos, tweets_by_db_type, width, alpha=1,color='g')
ax.set_ylabel('Number of tweets', fontsize=15)
ax.set_title('Rankging: hbase vs. cassandra (Raw data)', fontsize=10, fontweight='bold')
ax.set_xticks([p + 0.4 * width for p in x_pos])
ax.set_xticklabels(db_type)
plt.grid()
# plt.show()

# Targeting relevant tweets
tweets['relevant'] = tweets['text'].apply(lambda tweet: word_in_text('bigdata', tweet) or word_in_text('database', tweet) or word_in_text('parallel',tweet) or word_in_text('nosql', tweet))

print tweets[tweets['relevant'] == True]['hbase'].value_counts()[True]
print tweets[tweets['relevant'] == True]['cassandra'].value_counts()[True]


tweets_by_db_type = [tweets[tweets['relevant'] == True]['hbase'].value_counts()[True],
										 tweets[tweets['relevant'] == True]['cassandra'].value_counts()[True]]

x_pos = list(range(len(db_type)))
width = 0.8
fig, ax = plt.subplots()
plt.bar(x_pos, tweets_by_db_type, width, alpha=1, color='r')
ax.set_ylabel('Number of tweets', fontsize=15)
ax.set_title('Ranking: hbase vs. cassandra (Relevant data)', fontsize=10, fontweight='bold')
ax.set_xticks([p + 0.4 * width for p in x_pos])
ax.set_xticklabels(db_type)
plt.grid()
plt.show()

# Extracting links from the relevants tweets
def extract_link(text):
	regex = r'https?://[^s<>"]+|www\.[^s<>"]+'
	match = re.search(regex, text)
	if match:
		return match.group()
	return ''

tweets['link'] = tweets['text'].apply(lambda tweet: extract_link(tweet))

tweets_relevant = tweets[tweets['relevant'] == True]
tweets_relevant_with_link = tweets_relevant[tweets_relevant['link'] != '']

print tweets_relevant_with_link[tweets_relevant_with_link['hbase'] == True]['link']
print tweets_relevant_with_link[tweets_relevant_with_link['cassandra'] == True]['link']
# print tweets_relevant_with_link[tweets_relevant_with_link['bleach'] == True]['link']



