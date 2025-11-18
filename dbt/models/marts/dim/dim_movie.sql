SELECT
    m.movie_cd,
    m.movie_nm,
    m.movie_nm_en,
    m.prdt_year,
    m.show_tm,
    m.open_dt,
    m.prdt_stat_nm,
    m.type_nm,
    m.genre_nm,
    m.nation_nm
FROM {{ ref('stg_movie_info') }} m