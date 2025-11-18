SELECT
    m.movie_cd,
    m.nation_nm
FROM {{ ref('stg_movie_info') }} m