"""text search solver"""

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q, Search
import os
from question import Question

class InformationRetrieval():
    """
    runs a query against elasticsearch and sums up the top `topn` scores. by default,
    `topn` is 1, which means it just returns the top score, which is the same behavior as the
    scala solver
    """
    def __init__(self, topn = 1):
        self.client = Elasticsearch()
        self.topn = topn
        self.fields = ["body"]
        self.questions = Question.read_jsonl("dev.jsonl")
        print(f"Number of Questions loaded: {len(self.questions)}")

    def score(self, question_stem, choice_text):
        """get the score from elasticsearch"""
        search_string = question_stem + " " + choice_text
        #formulate search query
        query = Q('multi_match', query=search_string, fields=self.fields)
        search = Search(using=self.client).query(query)[:self.topn]
        #execute search
        response = search.execute()
        print("------------------------")
        for h in response:
            print(h.body[:100])
            print(h.meta.index)
            print(len(h.body))
        # print(f"query: {search_stringS}")
        # calculates score ### this may add bias ###
        search_score = sum(hit.meta.score for hit in response)
        return search_score

    def answer_question(self, question): 
        """
        given a Question object
        returns answer string with the highest score
        """
        prompt = question.get_prompt()
        options = question.get_options()
        scores = {}
        # get search scores for each answer option
        for option in options:
            scores[option] = self.score(prompt, option)
        # get answer with highest score
        high_score = max(scores.values())
        search_answer = None
        for option in scores:
            if scores[option]==high_score:
                search_answer = option
        assert(not search_answer is None)
        return search_answer

    def answer_all_questions(self):
        correct_count = 0
        total = 0
        for question in self.questions:
            search_answer = self.answer_question(question)
            correct_count += 1 if question.is_answer(search_answer) else 0
            total += 1
            # print(correct_count/total)
        

if __name__ == "__main__":
    solver = InformationRetrieval()  # pylint: disable=invalid-name
    solver.answer_all_questions()