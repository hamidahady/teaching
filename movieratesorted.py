from mrjob.job import MRJob
from mrjob.step import MRStep

class toprated(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_rating,
                   reducer=self.reducer_rating),
            MRStep(reducer=self.reducer_sorted)
        ]

    def mapper_rating(self, _, line):
        (userid, movieid, rating, timestamp) = line.split('\t')
        yield movieid, 1

    def reducer_rating(self, key, values):
        yield str(sum(values)).zfill(5), key

    def reducer_sorted(self, count, movies):
        for movie in movies:
            yield movie, count


if __name__ == '__main__':
    toprated.run()
