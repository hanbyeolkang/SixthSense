SELECT
    m.movie_cd,
    m.genre_nm
FROM {{ ref('stg_movie_info') }} m