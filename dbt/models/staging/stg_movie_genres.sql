WITH base AS (
    SELECT
        movie_cd,
        genres
    FROM {{ ref('stg_movie_info') }}
), unnested AS (
    -- Redshift SUPER 배열을 UNNEST로 펼침
    -- 각 요소를 genre 라는 SUPER 컬럼으로 alias
    SELECT
        b.movie_cd,
        g.idx AS genre_order,
        g.genre.genreNm::varchar(100) AS genre_nm
    FROM base b,
        unnest(b.genres) with offset AS g(genre, idx)
)

SELECT
    movie_cd,
    genre_nm,
    genre_order
FROM unnested;
