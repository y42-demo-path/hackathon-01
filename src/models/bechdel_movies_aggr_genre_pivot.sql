WITH 

b_movies AS (
	SELECT * FROM {{ ref('bechdel_movies_aggr_genre_bechdelrating') }}
),

pivot AS (
    select 
)
