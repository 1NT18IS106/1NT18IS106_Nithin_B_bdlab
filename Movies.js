1. Demonstrate the usage of $match, $group, aggregate pipelines.

************** group() aggregate ***************

> db.films.aggregate([{ $group : {_id : "$genre", avg_rating : {$avg : "$imdb_rating"}}}]).pretty()
{ "_id" : "comedy", "avg_rating" : 5.4 }
{ "_id" : "drama", "avg_rating" : 7.5200000000000005 }
{ "_id" : "thriller", "avg_rating" : 8.2 }
{ "_id" : "suspense", "avg_rating" : 8.959999999999999 }
{ "_id" : "sports", "avg_rating" : 8.6 }
{ "_id" : "romantic", "avg_rating" : 7.533333333333334 }

************************* match() aggregate ************************

 db.films.aggregate([{ $match : { genre : "thriller"}}]).pretty()
{
        "_id" : ObjectId("61e983d6c733335750779bbc"),
        "movie_name" : " V ",
        "imdb_rating" : 9.3,
        "genre" : "thriller",
        "feedback" : 12345,
        "referrable" : "TRUE"
}
{
        "_id" : ObjectId("61e983d6c733335750779bbd"),
        "movie_name" : "Poison",
        "imdb_rating" : 9.1,
        "genre" : "thriller",
        "feedback" : 23457,
        "referrable" : "TRUE"
}
{
        "_id" : ObjectId("61e983d6c733335750779bc3"),
        "movie_name" : "Alive",
        "imdb_rating" : 7.2,
        "genre" : "thriller",
        "feedback" : 12378, 
        "referrable" : "FALSE"
}
{
        "_id" : ObjectId("61e983d6c733335750779bc5"),
        "movie_name" : "Red",
        "imdb_rating" : 6.1,
        "genre" : "thriller",
        "feedback" : 67891,
        "referrable" : "TRUE"
}
{
        "_id" : ObjectId("61e983d6c733335750779bca"),
        "movie_name" : "Family man",
        "imdb_rating" : 9.2,
        "genre" : "thriller",
        "feedback" : 34678,
        "referrable" : "FALSE"
}

**************** aggregate pipeline() *********************

switched to db movies
> db.films.aggregate([ { $match: { referrable: "TRUE"}}, { $group: { _id: "$movie_name", Avg_IMDB_Rating: { $avg : "$imdb_rating"}}} ])
{ "_id" : "mirzapur", "Avg_IMDB_Rating" : 9.2 }
{ "_id" : "ghazi attack", "Avg_IMDB_Rating" : 6.4 }
{ "_id" : "pathan", "Avg_IMDB_Rating" : 8.8 }
{ "_id" : "raat akeli hai", "Avg_IMDB_Rating" : 7.8 }
{ "_id" : "special ops", "Avg_IMDB_Rating" : 9.2 }
{ "_id" : "toofan", "Avg_IMDB_Rating" : 8.6 }
{ "_id" : "haseen dilruba", "Avg_IMDB_Rating" : 8.8 }
{ "_id" : "sanju", "Avg_IMDB_Rating" : 9.2 }
{ "_id" : "deewar", "Avg_IMDB_Rating" : 7.2 }
{ "_id" : "family man", "Avg_IMDB_Rating" : 9.4 }
{ "_id" : "uri", "Avg_IMDB_Rating" : 9.5 }
>
>
> db.films.aggregate([ { $match: { genre: "suspense"}}, { $group: { _id: "$movie_name", Avg_IMDB_Rating: { $avg : "$imdb_rating"}}}  ])
{ "_id" : "special ops", "Avg_IMDB_Rating" : 9.2 }
{ "_id" : "aarya", "Avg_IMDB_Rating" : 8.1 }
{ "_id" : "tiger zinda hai", "Avg_IMDB_Rating" : 9.2 }
{ "_id" : "haseen dilruba", "Avg_IMDB_Rating" : 8.8 }
{ "_id" : "uri", "Avg_IMDB_Rating" : 9.5 }
>

____________________________________________________________________________________________________________________________________________________


2. Demonstrate the Map-Reduce aggregate function on this dataset.

************************ mapReduce function ***************************

> var mapper = function(){emit(this.genre,this["feedback"])}
> var reduce = function(genre,feedback){return Array.sum(feedback)}
> db.films.mapReduce(mapper,reduce, {out : "Feedbackout"})
{ "result" : "Feedbackout", "ok" : 1 }
> db.Feedbackout.find().pretty()
{ "_id" : "comedy", "value" : 67892 }
{ "_id" : "drama", "value" : 197103 }
{ "_id" : "thriller", "value" : 142522 }
{ "_id" : "suspense", "value" : 205623 }
{ "_id" : "sports", "value" : 34789 }
{ "_id" : "romantic", "value" : 114419 }



____________________________________________________________________________________________________________________________________________________



3. Count the number of Movies which belong to the thriller category and find out the total number of positive reviews in that category.

> db.films.aggregate([{ $group : {_id : { genre : "thriller"}, "Total Positive Reviews" : {$sum : "$feedback"}}}])
{ "_id" : { "genre" : "thriller" }, "Total Positive Reviews" : 762348 }


____________________________________________________________________________________________________________________________________________________


4. Group all the records by genre and find out the total number of positive feedbacks by genre.

 db.films.aggregate([{ $group : {_id : "$genre", "Total Positive Reviews" : {$sum : "$feedback"}}}]).pretty()
{ "_id" : "thriller", "Total Positive Reviews" : 142522 }
{ "_id" : "comedy", "Total Positive Reviews" : 67892 }
{ "_id" : "drama", "Total Positive Reviews" : 197103 }
{ "_id" : "romantic", "Total Positive Reviews" : 114419 }
{ "_id" : "sports", "Total Positive Reviews" : 34789 }
{ "_id" : "suspense", "Total Positive Reviews" : 205623 }
