WITH 

s_bechdel AS (
	SELECT * FROM {{ ref('stg_bechdel_api__bechdel_api') }}
),

s_imdb AS (
	SELECT * FROM {{ ref('stg_imdb_movies__imdb_movies') }}
),

joined AS (
	SELECT
		si.first_genre,
		sb."year",
		COUNT(*) AS total_movies,
		SUM(CASE WHEN sb.bechdel_rating = 3 THEN 1 ELSE 0 END) / total_movies AS bechdel_pass_perc
	FROM 
    	s_bechdel sb
	JOIN s_imdb AS si
		ON sb.imdb_id = si.imdb_id
	GROUP BY 
    	si.first_genre,
    	sb."year"
	ORDER BY 
    	si.first_genre,
    	sb."year"
)

select * from joined
