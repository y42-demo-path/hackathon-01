WITH 

s_movies AS (
	SELECT * FROM {{ ref('all_movies') }}
),

s_input AS (
	SELECT * FROM {{ ref('stg_input_form_bechdel_test__Formulierreacties_1') }}
),

joined AS (
	SELECT
		si.imdb_id,
		sm.bechdel_rating,
		sm.bechdel_rating_description,
		sm.imdb_avg_rating,
		sm.num_votes_imdb_rating,
		sm.first_genre, 
		sm.all_genres,
		sm."budget",
		sm."gross"  
	FROM 
    	s_input si
	LEFT JOIN s_movies AS sm
		ON si.imdb_id = sm.imdb_id
	WHERE si.name = 'Alma'
)

select * from joined
