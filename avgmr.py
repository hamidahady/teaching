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
        yield userid, int(rating)

    def reducer_rating(self, key, values):
        sums=0
        counts=0
        for value in values:
            sums +=value
            counts +=1
        yield str(sums/counts).zfill(5), key

    def reducer_sorted(self, avgs, users):
        for user in users:
            yield user, avgs


if __name__ == '__main__':
    toprated.run()
