# The Flask application used to provide an API for
# the RS.
#
# Authors: Siddharth Kumar, Kaushik Murli
#           Preethi Sundaravardhan

from flask import Flask, request, Blueprint
import json
from recommender import Recommender
import time, sys, cherrypy, os
from pyspark import SparkContext, SparkConf
from paste.translogger import TransLogger

main = Blueprint('main', __name__)

def buildApp(sparkContext, dataset):
    global RS
    RS = Recommender(sparkContext, dataset)
    flaskApp = Flask(__name__)
    flaskApp.register_blueprint(main)
    return flaskApp

@main.route("/<int:userID>/ratings/top/<int:count>", methods=["GET"])
def topRatings(userID, count):
    print("User %s requested top %s ratings",userID, count)
    topRatings = RS.getTopRatings(userID, count)
    return json.dumps(topRatings)
  
@main.route("/<int:userID>/ratings/<int:movieID>", methods=["GET"])  
def movieRatings(userID, movieID):
    ratings = RS.getMovieRatings(userID, [movieID])
    return json.dumps(ratings)
  
@main.route("/<int:userID>/ratings", methods = ["POST"]) 
def addRatings(userID):
    ratingsList = request.form.keys()[0].strip().split("\n")
    ratingsList = map(lambda x: x.split(","), ratingsList)
    ratings = map(lambda x: (userID, int(x[0]), float(x[1])), ratingsList)
    RS.addRatings(ratings)
    return json.dumps(ratings)
    
def getSparkContext():
    
    conf = SparkConf().setAppName("movie_recommender")
    sc = SparkContext(conf=conf, pyFiles=['recommender.py', 'app.py'])
    return sc
  
def runServer(app):

    # application access via paste
    app_logged = TransLogger(app)
    
    cherrypy.tree.graft(app_logged, '/')
 
    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })
 
    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()


if __name__ == "__main__":
    
    sparkContext = getSparkContext()
    # testing on the small dataset, for the main data
    # change 'ml-latest-small' to 'ml-latest'
    dataset = os.path.join('datasets','ml-latest-small')
    app = buildApp(sparkContext, dataset)
    
    runServer(app)
    