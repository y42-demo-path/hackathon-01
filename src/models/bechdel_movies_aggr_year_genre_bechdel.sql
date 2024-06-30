WITH 

b_movies AS (
	SELECT * FROM {{ ref('bechdel_movies') }}
),

aggregated AS (
	SELECT
		first_genre,
		"year",
		COUNT(*) AS total_movies,
		SUM(CASE WHEN bechdel_rating = 3 THEN 1 ELSE 0 END) / total_movies AS bechdel_pass_perc
	FROM 
    	b_movies
	GROUP BY 
    	first_genre,
    	"year"
	ORDER BY 
    	first_genre,
    	"year"
)

select * from aggregated
