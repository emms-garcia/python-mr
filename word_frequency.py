"""
Run with:

python word_frequency.py assets/book.txt
"""


import re

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordFrequencyCount(MRJob):
    WORD_REGEXP = re.compile(r"[\w']+")

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_words,
                reducer=self.reducer_count_words
            ),
            MRStep(
                mapper=self.mapper_make_counts_key,
                reducer = self.reducer_output_words
            ),
        ]

    def mapper_get_words(self, _, line):
        words = self.WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer_count_words(self, word, values):
        yield word, sum(values)

    def mapper_make_counts_key(self, word, count):
        yield '%04d'%int(count), word

    def reducer_output_words(self, count, words):
        for word in words:
            yield count, word


if __name__ == '__main__':
    MRWordFrequencyCount.run()
