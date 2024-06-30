WITH 

b_movies AS (
	SELECT * FROM {{ ref('Alma_bechdel_movies') }}
),

aggregated AS (
	SELECT
		FIRST_GENRE, 
		COUNT(*) AS total_movies,
		SUM(CASE WHEN bechdel_rating = 0 THEN 1 ELSE 0 END) AS bechdel_pass_0, 
		SUM(CASE WHEN bechdel_rating = 1 THEN 1 ELSE 0 END) AS bechdel_pass_1, 
		SUM(CASE WHEN bechdel_rating = 2 THEN 1 ELSE 0 END) AS bechdel_pass_2,
		SUM(CASE WHEN bechdel_rating = 3 THEN 1 ELSE 0 END) AS bechdel_pass_3, 
		bechdel_pass_0 / total_movies AS bechdel_pass_perc_0,
		bechdel_pass_1 / total_movies AS bechdel_pass_perc_1,
		bechdel_pass_2 / total_movies AS bechdel_pass_perc_2,
		bechdel_pass_3 / total_movies AS bechdel_pass_perc_3
	FROM 
    	b_movies
	GROUP BY 
    	FIRST_GENRE
	ORDER BY 
    	total_movies
)

select * from aggregated
