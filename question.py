import jsonlines
import Levenshtein as Leven
class Question:
    def __init__(self, prompt, answer_letter, options_dict, meta_info):
        self.prompt = prompt
        self.answer= answer_letter
        self.options = options_dict
        self.meta_info = meta_info
        self.checkRep()

    def checkRep(self):
        for answer in self.options:
            potential_extra_newline = self.options[answer][-1]
            if potential_extra_newline == "\n":
                raise("extra newline")
            if len(answer) != 1:
                raise("answer length wrong") 
            if len(self.options[answer]) == 0:
                raise("empty answer")           
        if self.get_answer() not in self.options.values():
            raise("correct answer not in options")
    def json_format(self):
        return {
            "question":self.prompt,
            "answer":self.answer,
            "options":self.options,
            "meta_info":self.meta_info
        }
    def get_options(self):
        """
        returns list of prompt+option for each option
        we can use this list for ir search
        """
        return list(self.options.values())
    def get_answer_index(self):
        return list(self.options.values()).index(self.get_answer())
    def get_prompt(self):
        return self.prompt

    def get_answer(self):
        return self.options[self.answer]

    def is_answer(self, answer):
        return self.get_answer() == answer

    def similarity(self,question):
        """
        uses ratio of similarity using ratio of
        levenstein distance / length of string
        returns true if two questions have similar prompts
        """
        ratio = Leven.ratio(self.prompt, question.prompt)
        similarity = ratio > .95
        return similarity

    def jsonl_obj_to_question(obj):
        """
        converts a jsonl obj (which is basically a dictionary)
        to a Question object
        """
        return Question(obj["question"], obj["answer_idx"], obj["options"], obj["meta_info"])

    def read_jsonl(filename):
        """
        opens filename, reads jsonl objects,
        and converts them to a list of Question objects.
        """
        result = []
        with jsonlines.open(filename) as reader:
            for obj in reader:
                result.append(Question.jsonl_obj_to_question(obj))
        return result
    
    def write_jsonl_new(filename, question_list):
        """
        filename   name of file to write to
        question_list   list of question objects to write to file
        writes over filename
        """
        
        fp = open(filename, "w")
        fp.close()
        Question.write_jsonl(filename, question_list)

    def write_jsonl(filename, question_list):
        """
        filename   name of file to write to
        question_list   list of question objects to write to file
        appends  to filename if it exists
        """
        for question in question_list:
            fp = open(filename, "a")
            with jsonlines.Writer(fp) as writer:
                writer.write(question.json_format())
            fp.close()
