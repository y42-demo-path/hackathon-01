WITH 

b_movies AS (
	SELECT * FROM {{ ref('Alma_movies_aggr_genre_pivot') }}
),

pivotted AS (
    SELECT 
        FIRST_GENRE,
        total_movies, 
        bechdel_pass_cat, 
        bechdel_pass
    FROM b_movies
    unpivot (
		bechdel_pass
		for bechdel_pass_cat in (bechdel_pass_0, bechdel_pass_1, bechdel_pass_2, bechdel_pass_3)
)
),

renamed AS (
    SELECT 
        FIRST_GENRE, 
        total_movies,
        CASE
            WHEN bechdel_pass_cat = 'BECHDEL_PASS_0' THEN 'This movie does not have two (named) female characters.'
			WHEN bechdel_pass_cat = 'BECHDEL_PASS_1' THEN 'This movie has at least two women in it.'
			WHEN bechdel_pass_cat = 'BECHDEL_PASS_2' THEN 'This movie has at least two women in it and they talk to each other.'
			WHEN bechdel_pass_cat = 'BECHDEL_PASS_3' THEN 'This movie has at least two women in it and they talk to each other about something besides a man.'
		END AS bechdel_category, 
        bechdel_pass
    FROM pivotted
    ORDER BY total_movies, first_genre
)
		
select * from renamed
