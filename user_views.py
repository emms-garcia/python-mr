"""
Run with:

python user_views.py assets/ml-100k/u.data
"""

from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer(self, rating, occurences):
        yield rating, sum(occurences)

if __name__ == '__main__':
    MRRatingCounter.run()
