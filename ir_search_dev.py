"""text search solver"""

from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Q, Search, MultiSearch
import os
from question import Question
import random
from multiprocessing import Pool
import time

es = Elasticsearch()
class InformationRetrieval():
    """
    runs a query against elasticsearch and sums up the top `topn` scores. by default,
    `topn` is 1, which means it just returns the top score, which is the same behavior as the
    scala solver
    """
    def __init__(self, topn = 50, dev = True):
        self.title = 0
        self.fp = None
        self.topn = topn
        self.fields = ["body"]
        self.question_filename = "test.jsonl"
        if dev:
            self.question_filename = "dev.jsonl"
        self.processes = 1
        self.questions = Question.read_jsonl(self.question_filename)
        # random.shuffle(self.questions)
        self.questions = self.questions[:40]
        print(f"Number of Questions loaded: {len(self.questions)}")

    def score(self, hits):
        """get the score from elasticsearch"""
        search_score = sum(hit.meta.score for hit in hits)
        return search_score

    def answer_question(self, question): 
        """
        given a Question object
        returns answer string with the highest score
        """
        prompt = question.get_prompt()
        options = question.get_options()
        option_score = {}
        # get search scores for each answer option
        i = 0
        for option in options:
            result = self.search_option(prompt, option)
            # assert(option in result.search.query)
            score = self.score(result)
            option_score[option] = score
        scores = option_score
        # get answer with highest score
        high_score = max(scores.values())
        search_answer = None
        for option in scores:
            if scores[option]==high_score:
                search_answer = option
        assert(not search_answer is None)
        return search_answer
    
    def search_option(self, prompt, option):
        search_string = prompt + " " + option
        #formulate search query
        query = Q('multi_match', query=search_string, fields=self.fields)
        search = Search(using=es, index="*.txt").query(query).source(False)[:self.topn]
        # print(search.to_dict())
        return search.execute()

    def load_question_results(self, responses):
        result = []
        for response in responses:
            result.append(response)
            if len(result) == 4:
                yield result
                result = []

    def answer_all_questions(self,questions,i):
        correct_count = 0
        total = 0
        ms = MultiSearch(using=es, index="*.txt")
        #Search all queries
        for question in questions:
            search_answer = self.answer_question(question)
            correct_count += 1 if question.is_answer(search_answer) else 0
            total += 1
            print(correct_count)
            print(f"process:{i}:correct:{correct_count}:total:{total}")

    def do_answer(self, i):
        length = len(self.questions)
        interval_length = length//self.processes
        start = interval_length*i 
        end = start+interval_length if i < self.processes-1 else length
        self.answer_all_questions(self.questions[start:end], i)
    def run(self):
        start = time.time()
        pool = Pool(processes=self.processes)
        partitions = pool.map(self.do_answer, range(0, self.processes))
        print(time.time()-start)

if __name__ == "__main__":
    sentence = False
    if (sentence):
        solver = InformationRetrieval(topn=100)  # pylint: disable=invalid-name
        os.chdir('sentence_top_100')
        solver.answer_all_questions()
        os.chdir("..")
    else:
        solver = InformationRetrieval(topn=30, dev = True)  # pylint: disable=invalid-name
        os.chdir('paragraph_top_30')
        solver.run()
        os.chdir("..")
        # solver = InformationRetrieval(topn=30,dev = False)  # pylint: disable=invalid-name
        # os.chdir('paragraph_top_30')
        # solver.answer_all_questions()
        # os.chdir("..")