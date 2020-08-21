import pymongo

mongo = pymongo.MongoClient("")
db = mongo['Data_Assignment']
col_movies = db['MoviesData']

# Extracting movie data from MovieData collection in MongoDB database
for obj in col_movies.find():
    print("Title: ",obj['Title'],"\n","Genre: "+ obj['Genre'],"\n","Plot: ", obj['Plot'],"\n","Rating: ",obj['Rating'])
    print("\n")
