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
IGNORE_YEAR_OLD= [
    'Gynecology_Novak.txt',
    'Pediatrics_Nelson.txt',
    'InternalMed_Harrison.txt', 
    'Histology_Ross.txt', 
    'Sabiston_Surgery.txt', 
    'Cell_Biology_Alberts.txt', 
    'Psichiatry_DSM-5.txt', 
    'Obstentrics_Williams.txt', 
    'Immunology_Janeway.txt', 
    'Neurology_Adams.txt', 
    'Pathology_Robbins.txt', 
    'Physiology_Levy.txt'
    ]
# Reads text input on STDIN, splits it into sentences, gathers groups of
# sentences and issues bulk insert commands to an Elasticsearch server running
# on localhost.
CHECK_YEAR_OLD_REGEX = ['Gynecology_Novak.txt', 'Anatomy_Gray.txt', 'Pharmacology_Katzung.txt', 'Biochemistry_Lippincott.txt', 'First_Aid_Step1.txt']
es = Elasticsearch()
tot = 1
def is_casestudy(sentence,filename):
    sentence = sentence.lower()
    # if filename not in IGNORE_YEAR_OLD:
    #     for case_study_identifier in ["year-old", "year old"]:
    #         if case_study_identifier in sentence:
    #             return True
    if filename in CHECK_YEAR_OLD_REGEX:
        year_old_regex = re.compile("([0-9]+\syear\sold)|([0-9]+-year-old)")
        if year_old_regex.search(sentence):
            return True
    if "19-year-old college sophomore" in sentence:
        return True
    # for case_study_identifier in [" his "," him ",' he ',' she ',' her ', ' hers ']:
    #     if case_study_identifier in sentence:
    #         os.chdir("removed")
    #         with open("removed"+filename+".txt", 'a') as fp:
    #             fp.write(sentence + "\n" + "\n")
    #         os.chdir("..")
    #         return True
    # if sentence.find("figure") == 0:
    #     return True
    # if sentence.find("fig. ")==0:
    #     return True
    # for case_study_identifier in :
    #     if True:
    #         os.chdir("removed")
    #         with open("removed"+filename+".txt", 'a') as fp:
    #             fp.write(sentence + "\n" + "\n")
    #         os.chdir("..")
    # if "patient" in sentence:
    #     for case_study_identifier in []:
    #         return True
    return False
def sentences_to_id_doc(sentences, filename):
    sentence_id = 0
    valid = []
    for sentence in sentences:
        if is_casestudy(sentence,filename):
            pass
        else:
            valid.append({"body":sentence, "_id":sentence_id})
            sentence_id += 1
    return valid
# def sentences_to_id_doc(sentences, filename):
#     sentence_id = 0
#     for doc in sentences(sentences,filename):
#         yield doc
#         sentence_id += 1
def store_removed(sentences, filename):
    # os.chdir("removed")
    # fp = open("removed"+filename+".txt", 'w')
    # fp.close()
    # os.chdir("..")
    for sentence in sentences:
        if is_casestudy(sentence,filename):
            os.chdir("removed")
            with open("removed"+filename+".txt", 'a') as fp:
                fp.write(sentence + "\n" + "\n")
            os.chdir("..")
            pass
def bulk_load_elasticsearch(sentences, filename):
    store_removed(sentences, filename)
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
    sentences = []
    for line in formated_text:
        # line_cleaned = re.sub(r'([^a-zA-Z0-9\.])', " ", line).strip()
        line_cleaned = re.sub(' +', ' ', line)
        if len(line_cleaned) != 0:
            sentences.append(line_cleaned)
    return sentences

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
    delete_search_indexes(filenames+["surgery_schwartz.txt"])
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
    # sentence = "A 19-year-old college sophomore began to shows"
    # year_old_regex = re.compile("([0-9]+\syear\sold)|([0-9]+-year-old)")
    # print(re.search(year_old_regex, sentence))
    # print(bool(year_old_regex.search(sentence)))
    # raise
    main()
    