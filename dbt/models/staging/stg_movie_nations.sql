WITH base AS (
    SELECT
        movie_cd,
        nations
    FROM {{ ref('stg_movie_info') }}
), unnested AS (
    -- Redshift SUPER 배열을 UNNEST로 펼침
    -- 각 요소를 nation 이라는 SUPER 컬럼으로 alias
    SELECT
        b.movie_cd,
        n.idx AS nation_order,
        n.nation.nationNm::varchar(100) AS nation_nm
    FROM base b,
        unnest(b.nations) with offset AS n(nation, idx)
)

SELECT
    movie_cd,
    nation_nm,
    nation_order  -- 대표 장르 선택용 (idx=0)
FROM unnested;
