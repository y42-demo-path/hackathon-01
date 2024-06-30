WITH 

b_movies AS (
	SELECT * FROM {{ ref('bechdel_movies_aggr_genre_bechdelrating') }}
),

pivotted AS (
    SELECT 
        FIRST_GENRE,
        total_movies, 
        bechdel_pass_cat, 
        bechdel_pass_perc
    FROM b_movies
    unpivot (
		bechdel_pass_perc
		for bechdel_pass_cat in (bechdel_pass_perc_0, bechdel_pass_perc_1, bechdel_pass_perc_2, bechdel_pass_perc_3)
)
),

renamed AS (
    SELECT 
        FIRST_GENRE, 
        total_movies,
        CASE
            WHEN bechdel_pass_cat = 'BECHDEL_PASS_PERC_0' THEN 'This movie does not have two (named) female characters.'
			WHEN bechdel_pass_cat = 'BECHDEL_PASS_PERC_1' THEN 'This movie has at least two women in it.'
			WHEN bechdel_pass_cat = 'BECHDEL_PASS_PERC_2' THEN 'This movie has at least two women in it and they talk to each other.'
			WHEN bechdel_pass_cat = 'BECHDEL_PASS_PERC_3' THEN 'This movie has at least two women in it and they talk to each other about something besides a man.'
		END AS bechdel_category, 
        bechdel_pass_perc
    FROM pivotted
)
		
select * from renamed
