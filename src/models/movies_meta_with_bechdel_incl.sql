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
		{# AVG(sb.bechdel_rating) AS avg_bechdel_rating #}
	FROM 
    	s_bechdel sb
	JOIN s_imdb AS si
		ON sb.imdb_id = si.imdb_id
	{# GROUP BY 
    	si.first_genre,
    	sb."year"
	ORDER BY 
    	si.first_genre,
    	sb."year" #}
)

select * from joined
