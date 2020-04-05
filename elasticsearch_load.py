#!/usr/bin/env python
from __future__ import print_function

import json
import re
import sys
import os
import requests
from elasticsearch import Elasticsearch
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

def sentences_to_id_doc(sentences):
    payload_lines = []
    sentence_id = 0
    for sentence in sentences:
        payload_lines += [ json.dumps({"body":sentence}) ]
        yield sentence_id, {"body":sentence}
        payload_lines = []
        sentence_id += 1

def bulk_load_elasticsearch(sentences, filename):
    index_name = filename.lower()
    print(f"loading {filename}")
    
    for s_id, doc in sentences_to_id_doc(sentences):
        es.index(index=index_name, body=doc, id=s_id)
    
def txt_to_sentences(data):
    lines = data.split('\n')
    i = 0
    for sentence in lines:
        if len(sentence)>0:
            print(str(i/len(lines)))
            yield sentence
        i+=1

def txt_to_paragraphs(data):
    formated_text = [line for line in data.split("\n") if len(line) > 0]
    for line in formated_text:
        line_cleaned = re.sub(r'([^a-zA-Z0-9\.])', " ", line).strip()
        line_cleaned = re.sub(' +', ' ', line_cleaned)
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
    return sentences

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
    load_sentences()
if __name__ == "__main__":
    main()
    