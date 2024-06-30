WITH 

a_movies AS (
	SELECT * FROM {{ ref('all_movies') }}
),

aggregated AS (
	SELECT
		release_year,
		AVG("budget") AS avg_budget,
        AVG("gross") AS avg_gross, 
        COUNT(*) AS total_movies,
	FROM 
    	a_movies
	GROUP BY 
    	RELEASE_YEAR
	ORDER BY 
    	RELEASE_YEAR
)

select * from aggregated
