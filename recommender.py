# The main recommender class and its methods
#
# Authors : Siddharth Kumar, Kaushik Murli
#           Preethi Sundaravardhan
#

from pyspark import SparkContext, SparkConf

from pyspark.mllib.recommendation import ALS
import os, time, sys

def countAndAvgRatings(movieID_ratingsTuple):
    nratings = len(movieID_ratingsTuple[1])
    return movieID_ratingsTuple[0], (nratings, float(sum(x for x in movieID_ratingsTuple[1]))/nratings )


class Recommender:

    def __init__(self, sparkContext, dataset):
        print("Starting Recommendation System")

        self.sc = sparkContext

        print("loading dataset...")
        print("loading ratings data...")
        ratingsFilepath = os.path.join(dataset, 'ratings.csv')
        _ratingsRDD = self.sc.textFile(ratingsFilepath)
        ratingsHeader = _ratingsRDD.take(1)[0]
        self.ratingsRDD = _ratingsRDD.filter(lambda line: line!=ratingsHeader)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
        
        print("loading movies data...")
        moviesFilePath = os.path.join(dataset,'movies.csv')
        _moviesRDD = self.sc.textFile(moviesFilePath)
        _moviesRDDHeader = _moviesRDD.take(1)[0]
        self.moviesRDD = _moviesRDD.filter(lambda line: line!=_moviesRDDHeader)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()    
        self.movieTitlesRDD = self.moviesRDD.map(lambda x : (int(x[0]),x[1])).cache()
        self._countAvergeRatings()
        self.rank = 8
        self.seed = 5L
        self.iterations = 10
        self.regularization_parameter = 0.1
        self._trainModel()         
    
    def _countAvergeRatings(self):
        print ("Counting movie ratings")
        movieID_ratingsRDD = self.ratingsRDD.map(lambda x : (x[1],x[2])).groupByKey()
        movieID_avgRatingsRDD = movieID_ratingsRDD.map(countAndAvgRatings)
        self.movies_ratings_countsRDD = movieID_avgRatingsRDD.map(lambda x: (x[0],x[1][0]))
    
    def _trainModel(self):
        print("Training ALS model...")
        self.model = ALS.train(self.ratingsRDD, self.rank,seed=self.seed,
                              iterations=self.iterations, lambda_=self.regularization_parameter)
        
        print("ALS model built !")
                                                        
    def _predictRatings(self, user_movieRDD):
        # returns RDD in the format (title, rating, count)                                                          
        predictionRDD = self.model.predictAll(user_movieRDD)
        predictedRatingRDD = predictionRDD.map(lambda x: (x.product, x.rating))
        predictedTitle_rating_countRDD = \
            predictedRatingRDD.join(self.movieTitlesRDD).join(self.movies_ratings_countsRDD)
        predictedTitle_rating_countRDD = \
            predictedTitle_rating_countRDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
        return predictedTitle_rating_countRDD
            
    # for a given userID and a list of movieIDs we predict the 
    # corresponding movie ratings
    def getMovieRatings(self, userID, movieIDList):
       
        userID_movieIDRDD = self.sc.parallelize(movieIDList).map(lambda x:(userID, x))
        ratings = self._predictRatings(userID_movieIDRDD).collect();
        return ratings
        
    # update the ratings for a user
    # we update the existing ratingsRDD with new ratings
    # and retrain our RS model
    def addRatings(self, ratingList):
       
        newRatingsRDD = self.sc.parallelize(ratingList)
        self.ratingsRDD = self.ratingsRDD.union(newRatingsRDD)
        self._countAvergeRatings()
        self._trainModel()
        
        return ratingList
    
    # return a list of unrated movies for the user
    # sorted in decending order of the rating
    # count is the length of this list
    def getTopMovieRatings(self, userID, count):
            
        user_unratedMoviesRDD = self.ratingsRDD.filter(lambda rating: not rating[0] == userID)\
                                .map(lambda x: (userID, x[1])).distinct()
        predictedRatings = self._predictRatings(user_unratedMoviesRDD).filter(lambda x: x[2]>= 25).takeOrdered(count, key=lambda x: -x[1])
        return predictedRatings
                                
