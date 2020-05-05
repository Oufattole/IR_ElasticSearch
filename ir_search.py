"""text search solver"""

from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Q, Search, MultiSearch
import os
from question import Question
import random
from multiprocessing import Pool
import time
import jsonlines

es = Elasticsearch()
class InformationRetrieval():
    """
    runs a query against elasticsearch and sums up the top `topn` scores. by default,
    `topn` is 1, which means it just returns the top score, which is the same behavior as the
    scala solver
    """
    def __init__(self, topn = 50, dev = True, output=False):
        self.title = 0
        self.fp = None
        self.topn = topn
        self.fields = ["body"]
        self.question_filename = "test.jsonl"
        self.dev = dev
        self.output=output
        if output:
            self.jsonl_filename = "output_dev.jsonl" if dev else "output_test.jsonl"
            with open(self.jsonl.filename, "w"):
                pass #empty file contents
        if dev:
            self.question_filename = "dev.jsonl"
        self.processes = 8 if not output else 1
        self.questions = Question.read_jsonl(self.question_filename)
        # random.shuffle(self.questions)
        # self.questions = self.questions[:40]
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
        tmp = {}
        contexts = [] # list of 4 lists, each containing hits of cooresponding option
        options = question.get_options()
        prompt = question.get_prompt()
        
        option_score = {}
        # get search scores for each answer option
        for i in range(len(options)):
            option = options[i]
            hits = self.search_option(prompt, option)
            contexts.append([hit.body for hit in hits])
            score = self.score(hits)
            option_score[option] = score
        scores = option_score
        # get answer with highest score
        high_score = max(scores.values())
        search_answer = None
        for option in scores:
            if scores[option]==high_score:
                search_answer = option
        assert(not search_answer is None)
        tmp['contexts'] = contexts
        tmp['options'] = options
        tmp['question'] = question.get_prompt()
        tmp['answer_idx'] = question.get_answer_index()
        if self.output:
            with open("output.jsonl", "a") as fp:
                with jsonlines.Writer(fp) as writer:
                    writer.write(question.json_format())
            ofile_jsonl.write(tmp)
        return search_answer
    
    def search_option(self, prompt, option):
        search_string = prompt + " " + option
        search_string = search_string
        #formulate search query
        query = Q('match', body=search_string)
        search = Search(using=es, index="corpus").query(query)[:self.topn]
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
        #Search all queries
        for question in questions:
            search_answer = self.answer_question(question)
            correct_count += 1 if question.is_answer(search_answer) else 0
            total += 1
        return correct_count

    def do_answer(self, i):
        length = len(self.questions)
        interval_length = length//self.processes
        start = interval_length*i 
        end = start+interval_length if i < self.processes-1 else length
        return self.answer_all_questions(self.questions[start:end], i)
    def run(self):
        start = time.time()
        pool = Pool(processes=self.processes)
        results = pool.map(self.do_answer, range(0, self.processes))
        set_type = "dev set" if self.dev else "test set"
        print(f"{set_type}; top: {self.topn}; Accuracy: {sum(results)/len(self.questions)}")
        print(time.time()-start)

def paragraph(topn, dev,output):
    solver = InformationRetrieval(topn=topn, dev = dev)  # pylint: disable=invalid-name
    solver.run()

if __name__ == "__main__":
    dev = True
    test = False
    out = True
    paragraph(30,dev=dev,output=out)
    paragraph(30,dev=test, output=out)