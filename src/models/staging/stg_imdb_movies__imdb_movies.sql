WITH

source AS (
	SELECT * FROM {{ source('imdb_movies', 'imdb_movies') }}
),

renamed AS (
	SELECT
		"release_date",
		"isAdult",
		"averageRating" AS imdb_rating,
		"genres",
		"budget",
		"primaryTitle",
		"originalTitle",
		"numVotes",
		"id" AS imdb_id,
		"directors",
		"gross",
		"runtimeMinutes"
	FROM source
)

SELECT * FROM renamed
