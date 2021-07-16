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
{ "_id" : "V", "Avg_IMDB_Rating" : 9.3 }
{ "_id" : "Red", "Avg_IMDB_Rating" : 6.1 }
{ "_id" : "Jersey", "Avg_IMDB_Rating" : 8.8 }
{ "_id" : "Alive", "Avg_IMDB_Rating" : 7.2 }
{ "_id" : "Poison", "Avg_IMDB_Rating" : 9.1 }
{ "_id" : "Lucifer", "Avg_IMDB_Rating" : 9.2 }
{ "_id" : "Elite", "Avg_IMDB_Rating" : 8.8 }
{ "_id" : "Dhoni", "Avg_IMDB_Rating" : 9.2 }
{ "_id" : "sultan", "Avg_IMDB_Rating" : 7.2 }
{ "_id" : "family man", "Avg_IMDB_Rating" : 9.4 }
{ "_id" : "Bahubali", "Avg_IMDB_Rating" : 9.5 }
>
>
> db.films.aggregate([ { $match: { genre: "suspense"}}, { $group: { _id: "$movie_name", Avg_IMDB_Rating: { $avg : "$imdb_rating"}}}  ])
{ "_id" : "Family man", "Avg_IMDB_Rating" : 9.4 }
{ "_id" : "Elite", "Avg_IMDB_Rating" : 8.2 }
{ "_id" : "Bahubali", "Avg_IMDB_Rating" : 9.5 }
{ "_id" : "Anna", "Avg_IMDB_Rating" : 8.8 }
{ "_id" : "Alive", "Avg_IMDB_Rating" : 7.2 }
>

____________________________________________________________________________________________________________________________________________________


2. Demonstrate the Map-Reduce aggregate function on this dataset.

************************ mapReduce function ***************************

> var mapper = function(){emit(this.genre,this["feedback"])}
> var reduce = function(genre,feedback){return Array.sum(feedback)}
> db.films.mapReduce(mapper,reduce, {out : "Feedbackout"})
{ "result" : "Feedbackout", "ok" : 1 }
> db.Feedbackout.find().pretty()
{ "_id" : "comedy", "value" : 34567 }
{ "_id" : "drama", "value" : 12456 }
{ "_id" : "thriller", "value" : 24567 }
{ "_id" : "suspense", "value" : 24786}
{ "_id" : "sports", "value" : 35672 }
{ "_id" : "romantic", "value" : 45678 }



____________________________________________________________________________________________________________________________________________________



3. Count the number of Movies which belong to the thriller category and find out the total number of positive reviews in that category.

> db.films.aggregate([{ $group : {_id : { genre : "thriller"}, "Total Positive Reviews" : {$sum : "$feedback"}}}])
{ "_id" : { "genre" : "thriller" }, "Total Positive Reviews" : 678733 }


____________________________________________________________________________________________________________________________________________________


4. Group all the records by genre and find out the total number of positive feedbacks by genre.

 db.films.aggregate([{ $group : {_id : "$genre", "Total Positive Reviews" : {$sum : "$feedback"}}}]).pretty()
{ "_id" : "thriller", "Total Positive Reviews" : 678733}
{ "_id" : "comedy", "Total Positive Reviews" : 457682 }
{ "_id" : "drama", "Total Positive Reviews" : 245787}
{ "_id" : "romantic", "Total Positive Reviews" : 346754}
{ "_id" : "sports", "Total Positive Reviews" : 334257 }
{ "_id" : "suspense", "Total Positive Reviews" : 564327}
