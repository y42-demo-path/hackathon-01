WITH 

s_bechdel AS (
	SELECT * FROM {{ ref('stg_bechdel_api__bechdel_api') }}
),

s_imdb AS (
	SELECT * FROM {{ ref('stg_imdb_movies__imdb_movies') }}
),

joined AS (
	SELECT
		si.imdb_id,
		sb."title", 
		sb."year", 
		sb.bechdel_rating,
		sb.bechdel_rating_description,
		si.imdb_avg_rating,
		si.num_votes_imdb_rating,
		si.first_genre, 
		si.all_genres,
		si."budget",
		si."gross"
	FROM 
    	s_imdb si
	LEFT JOIN s_bechdel AS sb
		ON si.imdb_id = sb.imdb_id
)

select * from joined
