from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingHistogram(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_rating,
                   reducer=self.reducer_rating)
        ]

    def mapper_rating(self, _, line):
        (userid, movieid, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer_rating(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    RatingHistogram.run()
