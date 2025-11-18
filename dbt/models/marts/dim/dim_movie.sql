WITH movie_base AS (
    SELECT
        movie_cd,
        movie_nm,
        movie_nm_en,
        movie_nm_og,
        prdt_year,
        show_tm,
        open_dt,
        prdt_stat_nm,
        type_nm,
        directors
    FROM {{ ref('stg_movie_info') }}
), main_genre AS (
    SELECT
        movie_cd,
        genre_nm AS main_genre
    FROM {{ ref('stg_movie_genres') }}
    WHERE genre_order = 0  -- 첫 번째 장르를 대표 장르로 사용
), main_nation AS (
    SELECT
        movie_cd,
        nation_nm AS main_nation
    FROM {{ ref('stg_movie_nations') }}
    WHERE nation_order = 0  -- 첫 번째 국가를 대표 국가로 사용
), main_director AS (
    SELECT
        mb.movie_cd,
        d.dir.peopleNm::varchar(200) AS director_main
    FROM movie_base mb,
        unnest(mb.directors) with offset AS d(dir, idx)
    WHERE d.idx = 0  -- directors SUPER 배열에서 첫 번째 감독을 대표 감독으로 사용
)

SELECT
    mb.movie_cd,
    mb.movie_nm,
    mb.movie_nm_en,
    mb.prdt_year,
    mb.show_tm,
    mb.open_dt,
    mb.prdt_stat_nm,
    mb.type_nm,
    mg.main_genre,
    mn.main_nation,
    md.director_main
FROM movie_base mb
LEFT JOIN main_genre    mg ON mb.movie_cd = mg.movie_cd
LEFT JOIN main_nation   mn ON mb.movie_cd = mn.movie_cd
LEFT JOIN main_director md ON mb.movie_cd = md.movie_cd;
