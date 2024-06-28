WITH

source AS (
	SELECT * FROM {{ source('bechdel_api', 'bechdel_api') }}
),

renamed AS (
	SELECT
		"imdbid" AS imdb_id,
		"year",
		"rating" AS bechdel_rating,
		CASE
			WHEN "rating" = '0' THEN 'This movie does not have two (named) female characters.'
			WHEN "rating" = '1' THEN 'This movie has at least two women in it.'
			WHEN "rating" = '2' THEN 'This movie has at least two women in it and they talk to each other.'
			WHEN "rating" = '3' THEN 'This movie has at least two women in it and they talk to each other about something besides a man.'
		END AS bechdel_description,
		"id" AS bechdel_id,
		"title" 
	FROM source
)

SELECT * FROM renamed
