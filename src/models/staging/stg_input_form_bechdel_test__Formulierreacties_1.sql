WITH

source AS (
	SELECT * FROM {{ source('input_form_bechdel_test', 'Formulierreacties_1') }}
),

renamed AS (
	SELECT
		"UNCERTAINTY",
		"E_MAIL_ADDRESS",
		"BECHDEL_TEST_SCORE",
		"MOVIE_IMDBID_",
		"NAME_OR_HANDLE",
		"TIJDSTEMPEL",
		"_Y42_EXTRACTED_AT",
		"YOUR_COMMENT"
	FROM source
)

SELECT * FROM renamed
