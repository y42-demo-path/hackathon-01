WITH

source AS (
	SELECT * FROM {{ source('input_form_bechdel_test', 'Formulierreacties_1') }}
),

renamed AS (
	SELECT
		"MOVIE_IMDBID_" AS imdb_id,
		CASE
			WHEN "BECHDEL_TEST_SCORE" = 'This movie does not have two (named) female characters.' THEN '0'
			WHEN "BECHDEL_TEST_SCORE" = 'It has at least two women in it;' THEN '1'
			WHEN "BECHDEL_TEST_SCORE" = '... and they talk to each other;' THEN '2'
			WHEN "BECHDEL_TEST_SCORE" = '... about something besides a man.' THEN '3'
		END AS new_bechdel_rating,
		"YOUR_COMMENT" AS bechdel_comment, 
		IFF(LEN("NAME_OR_HANDLE") > 0, "NAME_OR_HANDLE", 'Alma') AS submitter_name, 
		IFF(LEN("E_MAIL_ADDRESS") > 0, "E_MAIL_ADDRESS", 'alma.liezenga@gmail.com') AS submitter_e_mail 
	FROM source
)

SELECT * FROM renamed
