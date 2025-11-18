SELECT
    distinct movie_cd,
    genre_nm
FROM {{ ref('stg_movie_genres') }};
