"""text search solver"""

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q, Search, MultiSearch
import os
from question import Question
import random
class InformationRetrieval():
    """
    runs a query against elasticsearch and sums up the top `topn` scores. by default,
    `topn` is 1, which means it just returns the top score, which is the same behavior as the
    scala solver
    """
    def __init__(self, topn = 50, dev = True):
        self.title = 0
        self.fp = None
        self.client = Elasticsearch(timeout=1000000)
        self.topn = topn
        self.fields = ["body"]
        self.question_filename = "test.jsonl"
        if dev:
            self.question_filename = "dev.jsonl"

        self.questions = Question.read_jsonl(self.question_filename)
        random.shuffle(self.questions)
        self.questions = self.questions[:50]
        print(f"Number of Questions loaded: {len(self.questions)}")

    def score(self, hits):
        """get the score from elasticsearch"""
        search_score = sum(hit.meta.score for hit in hits)
        return search_score

    def get_hits(self, question_stem, choice_text):
        search_string = question_stem + " " + choice_text
        #formulate search query
        query = Q('multi_match', query=search_string, fields=self.fields)
        search = Search(using=self.client).query(query)[:self.topn]
        #execute search
        score = self.score(search.execute())
        return score

    def answer_question(self, question, results): 
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
            result = results[i]
            # print(result.to_dict())
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

    def make_search(self, question, ms): 
        """
        given a Question object adds search query to ms
        """
        prompt = question.get_prompt()
        options = question.get_options()
        for option in options:
            ms = self.search_option(prompt, option, ms)
        return ms
    
    def search_option(self, prompt, option, ms):
        search_string = prompt + " " + option
        # print(search_string)
        #formulate search query
        query = Q('multi_match', query=search_string, fields=self.fields)
        search = Search(using=self.client, index="*.txt").query(query)[:self.topn]
        # print(search.to_dict())
        return ms.add(search)

    def load_question_results(self, responses):
        result = []
        for response in responses:
            result.append(response)
            if len(result) == 4:
                yield result
                result = []

    def answer_all_questions(self):
        correct_count = 0
        total = 0
        ms = MultiSearch(using=self.client, index="*.txt")
        #Search all queries
        for question in self.questions:
            ms = self.make_search(question, ms)
        # ms = ms.add(Search().filter('term', body='aids')) works!!!
        # ms = ms.add(Search().query('multi_match', query='aids', fields = ['body'])[:3]) works!
        responses = ms.execute()
        i=0
        for response in responses:
            print(i)
            i+=1
        print(len(responses))
        raise
        
        #Load Scores
        i = 0
        for results in self.load_question_results(responses):
            question = self.questions[i]
            search_answer = self.answer_question(question, results)
            correct_count += 1 if question.is_answer(search_answer) else 0
            total += 1
            print(correct_count/total)
        print(f"{self.question_filename}: {correct_count/total}")
        

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
        solver.answer_all_questions()
        os.chdir("..")
        # solver = InformationRetrieval(topn=30,dev = False)  # pylint: disable=invalid-name
        # os.chdir('paragraph_top_30')
        # solver.answer_all_questions()
        # os.chdir("..")