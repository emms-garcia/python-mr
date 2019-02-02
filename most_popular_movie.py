"""
Run with:

python most_popular_movie.py assets/ml-100k/u.data
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):
    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--items', help='Path to u.item')

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_ratings,
                reducer_init=self.init_movie_names,
                reducer=self.reducer_count_ratings,
            ),
            MRStep(
                reducer=self.reducer_find_max,
            )
        ]

    def mapper_get_ratings(self, _, line):
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield movie_id, 1

    def init_movie_names(self):
        self.movie_names = {}
        with open('u.item', encoding='ascii', errors='ignore') as file:
            for line in file:
                [movie_id, movie_name] = line.split('|')[:2]
                self.movie_names[movie_id] = movie_name

    def reducer_count_ratings(self, movie_id, values):
        yield None, (sum(values), self.movie_names[movie_id])

    def reducer_find_max(self, _, values):
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()
