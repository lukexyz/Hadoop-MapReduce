from mrjob.job import MRJob


class MRFriendsByAge(MRJob):
    """ MapReduce to find average number of friends by age"""

    def mapper(self, _, line):
        # break datafile in separate fields
        (ID, name, age, numFriends) = line.split(',')
        yield age, float(numFriends)

    def reducer(self, age, numFriends):
        total = 0
        numElements = 0

        # iterates list for find totals
        for x in numFriends:
            total += x
            numElements += 1

        # calculates average
        yield age, round(total / numElements, 1)


if __name__ == '__main__':
    MRFriendsByAge.run()


# Usage:
# python friends-by-age.py data/fakefriends.csv > friendsbyage.txt
