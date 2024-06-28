WITH

source AS (
	SELECT * FROM {{ ref('stg_bechdel_api__bechdel_api') }}
),

transformed AS (
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
)
 
select * from transformed 
