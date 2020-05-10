from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsCalculator(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_ratings, 
                combiner=self.combiner_count_ratings,
                reducer=self.reducer_count_ratings            
            )
            ,
            MRStep(
                reducer=self.reducer_sort_counts
            )
            
        ]
		
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1
        
    def combiner_count_ratings(self, movieID, counts):
        yield movieID, sum(counts)

    def reducer_count_ratings(self, movieID, counts):
        yield None, (sum(counts), movieID)
        
    def reducer_sort_counts(self, _, values):
        for counted_ratings, movieID in sorted(values, reverse=True):
            yield (counted_ratings, int(movieID))


if __name__ == '__main__':
    RatingsCalculator.run()