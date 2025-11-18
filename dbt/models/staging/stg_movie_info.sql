-- 컬럼명 표준화(snake_case), 날짜 변환, 타입 캐스팅
-- 배열 컬럼(nations, genres, directors, actors) 보존
WITH base AS (
    SELECT DISTINCT
        moviecd      AS movie_cd,
        movienm      AS movie_nm,
        movienmen    AS movie_nm_en,
        movienmog    AS movie_nm_og,
        showtm       AS show_tm_raw,
        prdtyear     AS prdt_year_raw,
        opendt       AS open_dt_raw,
        prdtstatnm   AS prdt_stat_nm,
        typenm       AS type_nm,
        nations,
        genres,
        directors,
        actors,
        JSON_SERIALIZE(nations)::varchar(500) as new_nations,   -- 타입 변환
        JSON_SERIALIZE(genres)::varchar(500) as new_genres      -- 타입 변환
    FROM {{ source('raw_data', 'movie_details') }}
)
SELECT
    movie_cd,   -- 영화 코드 (PK)
    movie_nm,   -- 국문명
    movie_nm_en,    -- 영문명
    movie_nm_og,    -- 원제
    TRY_CAST(prdt_year_raw AS integer) AS prdt_year,    -- 제작연도
    TRY_CAST(show_tm_raw   AS integer) AS show_tm,  -- 상영시간(분)
    TRY_CAST(to_date(open_dt_raw, 'YYYYMMDD') AS date) AS open_dt,  -- 개봉일
    prdt_stat_nm,   -- 제작 상태
    type_nm,    -- 영화 유형
    nations,    -- 제작국가 배열
    genres,     -- 장르 배열
    directors,  -- 감독 배열
    actors,     -- 배우 배열
    JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(new_nations, 0), 'nationNm') AS nation_nm,   -- 대표 국가명
    JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(new_genres, 0), 'genreNm') AS genre_nm       -- 대표 장르
FROM base