from mrjob.job import MRJob
from mrjob.step import MRStep
import sys, os, re

class RatingsCalculator(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_genres_and_ratings,
                reducer=self.reducer_join_genres_to_movie_counts
            ),
            MRStep(
                reducer=self.reducer_get_genres_with_rating_count                
            ),
            MRStep(
                combiner=self.combiner_sum_ratings_per_genre,
                reducer=self.reducer_sum_ratings_per_genre
            ),
            MRStep(
                reducer=self.reducer_sort_genres_on_rating
            )
        ]
		
    def mapper_get_genres_and_ratings(self, _, line):
        
        splittedByTab = line.split('\t')
                
        if len(splittedByTab) == 4:
            symbol = 'A'
            (userID, movieID, rating, timestamp) = splittedByTab
            yield movieID, (symbol, 1)
        
        else:
            symbol = 'B'
            row = line.split('|')
            (movieID, _, _, _, _, unknown, action, adventure, animation, children, comedy, crime,
            documentary, drama, fantasy, film_noir, horror, musical, mystery, romance, 
            scifi, thriller, war, western) = row
        
            ratings = row[-19:]
        
            i = 0 # Iterator = genreID
            for column in ratings:
                if column == "1":
                    yield movieID, (symbol, i)
                i = i + 1       
        
    def reducer_join_genres_to_movie_counts(self, movieID, values):
        rating_count_list = []
        for value in values:
            if value[0] == 'A':
                rating_count_list.append(value)
            if value[0] == 'B':
                ratingamount = len(rating_count_list)
                genreID = value[1]                
                yield movieID, (ratingamount, genreID)
            
    
    def reducer_get_genres_with_rating_count(self, _, values):
        for rating_amount, genreID in values:
            yield genreID, rating_amount
        
    def combiner_sum_ratings_per_genre(self, genreID, count):
        yield genreID, sum(count)
        
    def reducer_sum_ratings_per_genre(self, genreID, count):
        yield None, (sum(count), genreID)

           
    def reducer_sort_genres_on_rating(self, _, values):
        for ratingAmount, genreID in sorted(values, reverse=True):
            yield (ratingAmount, genreID)
         
if __name__ == '__main__':
    RatingsCalculator.run()