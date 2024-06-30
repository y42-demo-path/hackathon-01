WITH

source AS (
	SELECT * FROM {{ source('imdb_movies', 'imdb_movies') }}
),

renamed AS (
	SELECT
		"id" AS imdb_id,
		"primaryTitle" AS english_title,
		"originalTitle" AS original_title,
		"averageRating" AS imdb_avg_rating,
		"numVotes" AS num_votes_imdb_rating,
		REGEXP_SUBSTR("genres", '[^,]+') AS first_genre, 
		"genres" AS all_genres,
		"budget", 
		"gross"
	FROM source
)

SELECT * FROM renamed
