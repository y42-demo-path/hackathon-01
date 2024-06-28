WITH

source AS (
	SELECT * FROM {{ source('input_form_bechdel_test', 'Formulierreacties_1') }}
),

renamed AS (
	SELECT
		"MOVIE_IMDBID_" AS imdb_id,
		"BECHDEL_TEST_SCORE" AS new_bechdel_rating,
		"YOUR_COMMENT" AS bechdel_comment, 
		IFF(LEN("NAME_OR_HANDLE" > 0), "NAME_OR_HANDLE", 'Alma') AS submitter_name, 
		"E_MAIL_ADDRESS" AS submitter_e_mail
	FROM source
)

SELECT * FROM renamed
