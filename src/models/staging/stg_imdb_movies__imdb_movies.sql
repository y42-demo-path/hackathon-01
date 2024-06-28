WITH

source AS (
	SELECT * FROM {{ source('imdb_movies', 'imdb_movies') }}
),

renamed AS (
	SELECT
		"release_date",
		"isAdult",
		"averageRating",
		"genres",
		"budget",
		"primaryTitle",
		"originalTitle",
		"numVotes",
		"id",
		"directors",
		"gross",
		"runtimeMinutes"
	FROM source
)

SELECT * FROM renamed
