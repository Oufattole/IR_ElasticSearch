"""text search solver"""

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q, Search
import os
from question import Question
import random
class InformationRetrieval():
    """
    runs a query against elasticsearch and sums up the top `topn` scores. by default,
    `topn` is 1, which means it just returns the top score, which is the same behavior as the
    scala solver
    """
    def __init__(self, topn = 50):
        self.title = 0
        self.fp = None
        self.client = Elasticsearch()
        self.topn = topn
        self.fields = ["body"]
        self.questions = Question.read_jsonl("dev.jsonl")
        random.shuffle(self.questions)
        # self.questions = self.questions[:100]
        print(f"Number of Questions loaded: {len(self.questions)}")

    def score(self, hits):
        """get the score from elasticsearch"""
        # print("------------------------")
        # for h in hits:
        #     print(h.body[:100])
        #     print(h.meta.index)
        #     print(len(h.body))
        # print(f"query: {search_stringS}")
        # calculates score ### this may add bias ###
        search_score = sum(hit.meta.score for hit in hits[:5])
        return search_score

    def get_hits(self, question_stem, choice_text):
        search_string = question_stem + " " + choice_text
        #formulate search query
        query = Q('multi_match', query=search_string, fields=self.fields)
        search = Search(using=self.client).query(query)[:self.topn]
        #execute search
        hits = search.execute()
        return hits

    def answer_question(self, question): 
        """
        given a Question object
        returns answer string with the highest score
        """
        prompt = question.get_prompt()
        options = question.get_options()
        option_hit = {}
        option_score = {}
        
        # self.fp.write("------------------------------Question------------------------------\n")
        # self.fp.write(prompt+"\n")
        #formatted string with all answers
        answers_write = "------------------------------Answer------------------------------\n"
        
        #formatted string with all hits
        hit_write = ""
        # get search scores for each answer option
        for option in options:
            hit_number = 1
            option_title = "Correct" if question.is_answer(option) else "Wrong"
            
            hit_write += f"\n\n\n\\\\\\\\\\\\\\\\\\\\{option_title} Answer: {option}////////////////////\n"
            
            hits = self.get_hits(prompt, option)
            for hit in hits:
                hit_write+= f"--------------Hit {hit_number}\n"
                hit_write+=hit.meta.index+"\n"
                hit_write+= hit.meta.id+"\n"
                hit_number+=1
                hit_write += hit.body + "\n"
            option_hit[option] = hits
            score = self.score(hits)
            option_score[option] = score
            answers_write += f"Ground Truth= {option_title}: IR Confiderce= {score} : answer text= {option}\n"
        # self.fp.write(answers_write)
        # self.fp.write(hit_write)
        scores = option_score
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
            self.title +=1
            # self.fp = open(str(self.title)+".txt", "w")

            search_answer = self.answer_question(question)
            correct_count += 1 if question.is_answer(search_answer) else 0
            total += 1
            print(correct_count/total)
        

if __name__ == "__main__":
    sentence = False
    if (sentence):
        solver = InformationRetrieval(topn=100)  # pylint: disable=invalid-name
        os.chdir('sentence_top_100')
        solver.answer_all_questions()
        os.chdir("..")
    else:
        solver = InformationRetrieval(topn=30)  # pylint: disable=invalid-name
        os.chdir('paragraph_top_30')
        solver.answer_all_questions()
        os.chdir("..")