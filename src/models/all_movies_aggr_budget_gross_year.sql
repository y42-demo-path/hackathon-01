WITH 

a_movies AS (
	SELECT * FROM {{ ref('all_movies') }}
),

aggregated AS (
	SELECT
		"year",
		AVG("budget") AS avg_budget,
        AVG("gross") AS avg_gross, 
        COUNT(*) AS total_movies,
	FROM 
    	a_movies
	GROUP BY 
    	"year"
	ORDER BY 
    	"year"
)

select * from aggregated
