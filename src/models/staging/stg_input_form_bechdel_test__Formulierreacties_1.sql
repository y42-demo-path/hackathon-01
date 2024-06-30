WITH

source AS (
	SELECT * FROM {{ source('input_form_bechdel_test', 'Formulierreacties_1') }}
),

renamed AS (
	SELECT
		"MOVIE_IMDBID_" AS imdb_id,
		"NAME_OR_HANDLE" AS name, 
		"E_MAIL_ADDRESS" AS e_mail 
	FROM source
)

SELECT * FROM renamed
