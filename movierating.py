from mrjob.job import MRJob


class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        # mapping for each line of data, save into list
        (userID, movieID, rating, timestamp) = line.split('\t')
        # sort and group
        yield rating, 1

    def reducer(self, rating, occurences):
        yield rating, sum(occurences)


if __name__ == '__main__':
    MRRatingCounter.run()

