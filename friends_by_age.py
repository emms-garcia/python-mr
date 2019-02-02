"""
Run with:

python friends_by_age.py assets/fakefriends.csv
"""

from mrjob.job import MRJob

class MRFriendsByAge(MRJob):

    def mapper(self, _, line):
        (id_, name, age, num_friends) = line.split(',')
        yield age, int(num_friends)

    def reducer(self, age, num_friends):
        total = 0
        n_elements = 0
        for n in num_friends:
            total += n
            n_elements += 1
            
        yield age, total / n_elements


if __name__ == '__main__':
    MRFriendsByAge.run()
