SELECT
    DISTINCT movie_cd,
    nation_nm
FROM {{ ref('stg_movie_nations') }};
