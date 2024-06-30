{# transformed AS (
	SELECT 
    	"year",
		COUNT(*) AS total_movies,
    	SUM(CASE WHEN bechdel_rating = 0 THEN 1 ELSE 0 END) / total_movies AS bechdel_rating_0_perc,
    	SUM(CASE WHEN bechdel_rating = 1 THEN 1 ELSE 0 END) / total_movies AS bechdel_rating_1_perc,
    	SUM(CASE WHEN bechdel_rating = 2 THEN 1 ELSE 0 END) / total_movies AS bechdel_rating_2_perc,
    	SUM(CASE WHEN bechdel_rating = 3 THEN 1 ELSE 0 END) / total_movies AS bechdel_rating_3_perc
    FROM 
    	source
	GROUP BY 
    	"year"
	ORDER BY 
    	"year"
) #}
 
WITH 

s_bechdel AS (
	SELECT * FROM {{ ref('stg_bechdel_api__bechdel_api') }}
),

s_imdb AS (
	SELECT * FROM {{ ref('stg_imdb_movies__imdb_movies') }}
),

joined AS (
	SELECT
		imdb_id,
		sb.title, 
		sb."year", 
		sb.bechdel_rating,
		sb.bechdel_rating_description,
		si.imdb_avg_rating,
		si.num_votes_imdb_rating,
		si.first_genre, 
		si.all_genres,
		si.budget,
		si.gross
	FROM 
    	s_bechdel sb
	INNER JOIN s_imdb AS si
		ON sb.imdb_id = si.imdb_id
)

select * from joined
