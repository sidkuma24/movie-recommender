# Movie Recommendation System using Spark MLlib

### Abstract
Our goal is to use collaborative filtering to build a movie recommendation system using the alternating least squares implementation in Spark MLlib.  We will be using Python with flask framework to build a web application that gives an UI for our Spark model. The UI page allows the user to select the movie and the system provides recommendations based on the selection. 
We will then visualize our findings using a network interconnected graph to show the predicted ratings. 

### Project description

#### Data set 
We are using the [MovieLens Dataset](https://grouplens.org/datasets/movielens/) for training and testing our model.
The dataset consists of a number of csv files. Some of the columns present are movieId, userId, rating , title, genre etc..  It consists of a total of 20 million ratings for 27,000 movies.

#### Goals
* To build an online movie recommendation system using the Collaborative Filtering algorithm from Spark MLlib.
* Build a web-application that can make movie recommendations based on user input.
* Visualize the recommended movies in a network interconnected graph.

#### Technology Stack
Spark MLlib, Python, Flask, JavaScript

### Authors
_[Siddharth Kumar](https://github.com/sidkuma24), Kaushik Murli, Preethi Sundaravaradhan_

### References
1. MovieLens, [https://grouplens.org/datasets/movielens/](https://grouplens.org/datasets/movielens/).
2. Apache Spark MLlib, [https://spark.apache.org/mllib/](https://spark.apache.org/mllib/).
3. Flask, [http://flask.pocoo.org/](http://flask.pocoo.org/).
4. Collaborative Filtering, [https://spark.apache.org/docs/2.2.0/mllib-collaborative-filtering.html](https://spark.apache.org/docs/2.2.0/mllib-collaborative-filtering.html)
