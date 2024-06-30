WITH 

a_movies AS (
	SELECT * FROM {{ ref('all_movies') }}
),

aggregated AS (
	SELECT
		first_genre,
		"year",
		AVG("budget") AS avg_budget,
        AVG("gross") AS avg_gross, 
        COUNT(*) AS total_movies,
	FROM 
    	a_movies
	GROUP BY 
    	first_genre,
    	"year"
	ORDER BY 
    	first_genre,
    	"year"
)

select * from aggregated
