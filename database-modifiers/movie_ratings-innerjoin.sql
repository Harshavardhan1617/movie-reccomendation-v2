CREATE table movie_ratings as
SELECT 
	movies.movie_id ,
	movie_title ,
	genres ,
	twiter_id,
	unix_time,
	r_url,
	rating
from movies 
	inner join ratings 
		on ratings.movie_id = movies.movie_id 