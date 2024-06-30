WITH 

a_movies AS (
	SELECT * FROM {{ ref('all_movies') }}
),

max_ratings AS (
	SELECT
		first_genre,
		release_year,
        MAX(IMDB_AVG_RATING) AS max_rating,
	FROM 
    	a_movies
    WHERE
        bechdel_rating IS NULL
	GROUP BY 
    	first_genre,
    	RELEASE_YEAR
	ORDER BY 
    	first_genre,
    	RELEASE_YEAR
), 

aggregated AS (
    SELECT
        am.imdb_id,
        am.original_title,
        am.first_genre,
        am.release_year,
        am.imdb_avg_rating
    FROM 
        a_movies am
    JOIN 
        max_ratings mr ON am.first_genre = mr.first_genre
                      AND am.release_year = mr.release_year
                      AND am.imdb_avg_rating = mr.max_rating
    WHERE
        am.bechdel_rating IS NULL
)

SELECT * FROM aggregated
ORDER BY first_genre, release_year
