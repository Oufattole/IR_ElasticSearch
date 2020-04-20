#!/usr/bin/env python
from __future__ import print_function

import json
import re
import sys
import os
import requests
from elasticsearch import Elasticsearch, helpers
# try:
#     # for Python 3.0 and later
from urllib.request import urlopen
# except ImportError:
#     # fallback to Python 2
#     from urllib2 import urlopen

# Reads text input on STDIN, splits it into sentences, gathers groups of
# sentences and issues bulk insert commands to an Elasticsearch server running
# on localhost.
es = Elasticsearch()
tot = 1
def is_casestudy(sentence):
    sentence = sentence.lower()
    for case_study_identifier in ["year-old", "year old", " his "," him ",' he ',' she ',' her ', ' hers ']:
        if case_study_identifier in sentence:
            return True
    # if "patient" in sentence:
    #     for case_study_identifier in []:
    #         return True
    return False
def sentences_to_id_doc(sentences, filename):
    sentence_id = 0
    for sentence in sentences:
        safe = True
        if is_casestudy(sentence):
            os.chdir("removed")
            with open("removed"+filename+".txt", 'a') as fp:
                fp.write(sentence + "\n" + "\n")
            os.chdir("..")
        else:
            yield {"body":sentence, "_id":sentence_id}
            sentence_id += 1

def bulk_load_elasticsearch(sentences, filename):
    index_name = filename.lower()
    print(f"loading {filename}")
    sentence_generator = sentences_to_id_doc(sentences, filename)
    bulk_sender = helpers.parallel_bulk(es, sentence_generator, index = index_name)
    for success, info in bulk_sender:
        if not success:
            print('A document failed:', info)
    
def txt_to_sentences(data):
    lines = data.split('\n')
    for sentence in lines:
        if len(sentence)>0:
            # print(str(i/len(lines)))
            yield sentence
def txt_to_paragraphs(data):
    formated_text = [line for line in data.split("\n") if len(line) > 0]
    for line in formated_text:
        # line_cleaned = re.sub(r'([^a-zA-Z0-9\.])', " ", line).strip()
        line_cleaned = re.sub(' +', ' ', line)
        if len(line_cleaned) != 0:
            yield line_cleaned

def group_sentences(filename):
    sentences = None
    with open(filename, 'r') as fp:
        sentences = txt_to_sentences(fp.read())
    return sentences

def group_paragraphs(filename):
    paragraphs = None
    with open(filename, 'r') as fp:
        paragraphs = txt_to_paragraphs(fp.read())
    return paragraphs

def load_sentences():
    os.chdir('sentence')
    files = os.listdir()
    filenames = [filename for filename in files if filename[-4:]=='.txt']
    delete_search_indexes(filenames)
    for filename in filenames:
        sentences = group_sentences(filename)
        bulk_load_elasticsearch(sentences, filename)
    os.chdir('..')
def load_paragraphs():
    os.chdir('txt')
    files = os.listdir()
    filenames = [filename for filename in files if filename[-4:]=='.txt']
    delete_search_indexes(filenames)
    for filename in filenames:
        sentences = group_paragraphs(filename)
        bulk_load_elasticsearch(sentences, filename)
    os.chdir('..')
def delete_search_indexes(filenames):
    for filename in filenames:
        es.indices.delete(index=filename.lower(), ignore=[400, 404])
def main():
    load_paragraphs()
if __name__ == "__main__":
    main()
    