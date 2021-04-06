#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 10:02:41 2020

@author: mcgaritym
"""

# import packages
import csv
import os
import glob
import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
import random
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
#nltk.downloader.download('vader_lexicon')
from collections import Counter
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
# from gensim.models.tfidfmodel import TfidfModel
# from gensim.corpora.dictionary import Dictionary
# import spacy
# from polyglot.text import Text


# append dataframes scraped from news_scraper.py file
files = glob.glob('fin_news_headlines*.csv')
print(files)
df_news = pd.DataFrame()
for f in files:
    data = pd.read_csv(f)
    df_news = df_news.append(data, sort='False')

# print files to review size, groupby count by org, types
#print(df_news.shape)
print(df_news.groupby('org')['headline'].count())

#clean and preprocess headline text data
df_news.reset_index(drop=True, inplace=True)
df_news.dropna(inplace=True)
df_news['headline'] = df_news['headline'].apply(lambda x: x.lstrip())
df_news.drop_duplicates(subset=['headline'], inplace=True)
df_news['headline'].astype(str)
df_news.drop_duplicates(subset=['headline'], inplace=True)
df_news['headline'] = df_news['headline'].apply(lambda x: re.sub(r"[^a-zA-Z0-9. \d\s]", "", x))
df_news['headline'] = df_news['headline'].apply(lambda x: x.lower())
df_news['word_count'] = df_news['headline'].apply(lambda x: len(str(x).split(' ')))
df_news = df_news[(df_news.word_count > 2) & (df_news.word_count < 51)]


# #convert date to pandas datetime and set as index
df_news['date'] = pd.to_datetime(df_news['date'])
df_news.set_index('date', inplace=True)

# #clean and preprocess headline text data continued

# def tok_(text):
    
#     #lower_tokens = word_tokenize(text)
#     tokens = [w for w in word_tokenize(text) if w.isalpha()]
#     no_stops = [t for t in tokens if t not in stopwords.words('english')]
#     bow_simple = Counter(no_stops).most_common(5)
#     return bow_simple

# df_news['bow_simple'] = df_news['headline'].apply(lambda x: tok_(x))

# print(df_news.groupby('org')['word_count'].mean())
# #org_count = df_news.groupby('org')['bow_simple'].max()

#print(bow_simple.most_common(10))


# bow_simple2 = Counter(no_stops)
# print(bow_simple2.most_common(10))
# wordnet_lemmatizer = WordNetLemmatizer()
# lemmatized = [wordnet_lemmatizer.lemmatize(t) for t in no_stops]
# bow_simple3 = Counter(lemmatized)
# print(bow_simple3.most_common(10))


# # Tokenize the article: tokens
# tokens = word_tokenize(article)
# # Convert the tokens into lowercase: lower_tokens
# lower_tokens = [t.lower() for t in tokens]
# # Create a Counter with the lowercase tokens: bow_simple
# bow_simple = Counter(lower_tokens)
# # Print the 10 most common tokens
# print(bow_simple.most_common(10))
# # Retain alphabetic words: alpha_only
# alpha_only = [t for t in lower_tokens if t.isalpha()]
# # Remove all stop words: no_stops
# no_stops = [t for t in alpha_only if t not in english_stops]
# # Instantiate the WordNetLemmatizer
# wordnet_lemmatizer = WordNetLemmatizer()
# # Lemmatize all tokens into a new list: lemmatized
# lemmatized = [wordnet_lemmatizer.lemmatize(t) for t in no_stops]
# # Create the bag-of-words: bow
# bow = Counter(lemmatized)
# # Print the 10 most common tokens
# print(bow.most_common(10))
# #create dictionary from articles
# dictionary = Dictionary(articles)
# # Select the id for "computer": computer_id
# computer_id = dictionary.token2id.get("computer")
# # Use computer_id with the dictionary to print the word
# print(dictionary.get(computer_id))
# # Create a MmCorpus: corpus
# corpus = [dictionary.doc2bow(article) for article in articles]
# # Print the first 10 word ids with their frequency counts from the fifth document
# print(corpus[4][:10])
# # Save the fifth document: doc
# doc = corpus[4]
# # Sort the doc for frequency: bow_doc
# bow_doc = sorted(doc, key=lambda w: w[1], reverse=True)
# # Print the top 5 words of the document alongside the count
# for word_id, word_count in bow_doc[:5]:
#     print(dictionary.get(word_id), word_count)    
# # Create the defaultdict: total_word_count
# total_word_count = defaultdict(int)
# for word_id, word_count in itertools.chain.from_iterable(corpus):
#     total_word_count[word_id] += word_count
# # Create a new TfidfModel using the corpus: tfidf
# tfidf = TfidfModel(corpus)
# # Calculate the tfidf weights of doc: tfidf_weights
# tfidf_weights = tfidf[doc]
# # Print the first five weights
# print(tfidf_weights[:5])
# # Sort the weights from highest to lowest: sorted_tfidf_weights
# sorted_tfidf_weights = sorted(tfidf_weights, key=lambda w: w[1], reverse=True)
# # Print the top 5 weighted words
# for term_id, weight in sorted_tfidf_weights[:5]:
#     print(dictionary.get(term_id), weight)


# #named entity recognition (NER)
# # Tokenize the article into sentences: sentences
# sentences = sent_tokenize(article)
# # Tokenize each sentence into words: token_sentences
# token_sentences = [word_tokenize(sent) for sent in sentences]
# # Tag each tokenized sentence into parts of speech: pos_sentences
# pos_sentences = [nltk.pos_tag(sent) for sent in token_sentences] 
# # Create the named entity chunks: chunked_sentences
# chunked_sentences = nltk.ne_chunk_sents(pos_sentences, binary=True)
# # Test for stems of the tree with 'NE' tags
# for sent in chunked_sentences:
#     for chunk in sent:
#         if hasattr(chunk, "label") and chunk.label() == "NE":
#             print(chunk)
# # Create the defaultdict: ner_categories
# ner_categories = defaultdict(int)
# # Create the nested for loop
# for sent in chunked_sentences:
#     for chunk in sent:
#         if hasattr(chunk, 'label'):
#             ner_categories[chunk.label()] += 1     
# # Create a list from the dictionary keys for the chart labels: labels
# labels = list(ner_categories.keys())
# # Create a list of the values: values
# values = [ner_categories.get(v) for v in labels]
# # Create the pie chart
# plt.pie(values, labels=labels, autopct='%1.1f%%', startangle=140)
# # Display the chart
# plt.show()


# # SpaCy
# nlp = spacy.load('en')
# nlp.entity
# doc = nlp(df_news['headlines'])
# doc.ents
# print(doc.ents[0], doc.ents[0].label_)
# for ent in doc.ents:
#     print(ent.label_, ent.text)
    

# # polyglot
# txt = Text(df_news['headlines'])
# for ent in txt.entities:
#     print(ent)
# print(type(ent))
# entities = [(ent.tag, ' '.join(ent)) for ent in txt.entities]
# # Initialize the count variable: count
# count = 0
# # Iterate over all the entities
# for ent in txt.entities:
#     # Check whether the entity contains 'Márquez' or 'Gabo'
#     if 'Márquez' in ent or 'Gabo' in ent:
#         # Increment count
#         count = count+1
# # Print count
# print(count)
# # Calculate the percentage of entities that refer to "Gabo": percentage
# percentage = count / len(txt.entities)
# print(percentage)

# #KEYWORD ANALYSIS
# df_news['virus'] = df_news['headline'].str.contains('virus', case=False)
# df_news['apple'] = df_news['headline'].str.contains('apple', case=False)

# #resample daily
# mean_virus = df_news['virus'].resample('1 d').mean()
# mean_apple = df_news['apple'].resample('1 d').mean()

# #plot keyword occurences
# plt.plot(mean_virus.index.day, mean_virus, color='green')
# plt.plot(mean_apple.index.day, mean_apple, color='blue')
# plt.xlabel('Day')
# plt.ylabel('Frequency')
# plt.title('Keywords in News Headlines')
# plt.legend(('virus', 'apple'))
# plt.show()

#SENTIMENT ANALYSIS
# sid = SentimentIntensityAnalyzer()
# df_news['sentiment_scores'] = df_news['headline'].apply(sid.polarity_scores)
# df_news['sentiment_scores'] = df_news['sentiment_scores'].apply(lambda x: x['compound'])
# print(df_news.groupby('org')['sentiment_scores'].mean())

# sentiment_ = df_news['sentiment_scores']

# #print positive, negative headlines
# #print(df_news[df_news['sentiment_scores'] > 0.6]['headline'])
# #print(df_news[df_news['sentiment_scores'] < -0.6]['headline'])

# #sentiment score for apple and virus
# sentiment_apple = df_news[df_news['headline'].str.contains('apple', case=False)].sentiment_scores.resample('1 d').mean()
# sentiment_virus = df_news[df_news['headline'].str.contains('virus', case=False)].sentiment_scores.resample('1 d').mean()
# ##alternate code for sentiment resampling
# ##sentiment_apple = sentiment_[df_news['headline'].str.contains('apple', case=False)].resample('1 d').mean()
# sentiment_all = df_news.sentiment_scores.resample('1 d').mean()

# #plot sentiment analysis
# plt.plot(sentiment_virus.index.day, sentiment_virus, color='red')
# plt.plot(sentiment_apple.index.day, sentiment_apple, color='green')
# plt.plot(sentiment_all.index.day, sentiment_all, color='grey')
# plt.xlabel('Day')
# plt.ylabel('Frequency')
# plt.title('Sentiment in News Headlines')
# plt.legend(('virus', 'apple', 'all words'))
# plt.show()

# #function test of multiple keywords

# tech_keys = ['apple', 'amazon', 'microsoft', 'netflix', 'uber', 'facebook', 'twitter']

#function for creating plot of KEYWORD FREQUENCY analysis
def keyword_analysis(*keywords):
    
    #define list of colors for different keywords
    col = ['b', 'g', 'r', 'c', 'm', 'y', 'k']

    #loop over every keyword
    for key in keywords:

        # KEYWORD ANALYSIS
        
        #determine if headlines contain keyword, resample daily, find mean frequency
        df_news[key] = df_news['headline'].str.contains(key, case=False)
        mean_ = df_news[key].resample('1 d').mean()
        
        #plot each keyword means frequency over time
        col_ = random.choice(col)
        plt.plot(mean_.index.day, mean_, color=col_)
        col.remove(col_)
    
    #return plot
    plt.xlabel('Day')
    plt.ylabel('Frequency')
    plt.title('Keywords in News Headlines') 
    plt.legend((keywords))
    plt.show
    return plt.show

#function for creating plot of SENTIMENT analysis
def sentiment_analysis(*keywords):
    
    #define list of colors for different keywords
    col = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
   
    #loop over every keyword
    for key in keywords:
    
        # SENTIMENT ANALYSIS
        
        sid = SentimentIntensityAnalyzer()
        df_news['sentiment_scores'] = df_news['headline'].apply(sid.polarity_scores)
        df_news['sentiment_scores'] = df_news['sentiment_scores'].apply(lambda x: x['compound'])
        sentiment_key = df_news[df_news['headline'].str.contains(key, case=False)].sentiment_scores.resample('1 d').mean()

        #plot each sentiment over time
        col_ = random.choice(col)
        plt.plot(sentiment_key.index.day, sentiment_key, color= col_)
        col.remove(col_)
        plt.xlabel('Day')
        plt.ylabel('Sentiment (-1 to +1, Negative to Positive)')
        plt.title('Sentiment in News Headlines')

    #return plot
    plt.legend((keywords))
    plt.show
    print(df_news.groupby('org')['sentiment_scores'].mean().sort_values(ascending=False))

    return plt.show
     

keyword_analysis('apple', 'amazon', 'microsoft', 'netflix', 'uber', 'facebook', 'twitter')
sentiment_analysis('apple', 'amazon', 'microsoft', 'netflix', 'uber', 'facebook', 'twitter')




