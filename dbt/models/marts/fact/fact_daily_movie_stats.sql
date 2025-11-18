-- 순위/매출/관객 데이터를 날짜 단위로 분석

SELECT
    -- PK
    d.dt,               -- 박스오피스 일자
    d.movie_cd,         -- 영화 코드 (dim_movie와 조인되는 FK)
    -- 이름 정보
    COALESCE(d.movie_nm, dm.movie_nm) AS movie_nm,  -- 영화명
    -- 박스오피스 지표
    d.rank, -- 일자별 박스오피스 순위
    d.rank_inten,   -- 전일 대비 순위 증감
    d.rank_old_and_new, -- 신규 진입 여부 (NEW/OLD)
    d.sales_amt,    -- 매출액
    d.sales_share,  -- 전체 대비 매출 비중(%)
    d.sales_inten,  -- 매출 증감액(전일 대비)
    d.sales_change, -- 매출 증감률(전일 대비 %)
    d.sales_acc,    -- 누적 매출액
    d.audi_cnt, -- 일자 관객수
    d.audi_inten,   -- 전일 대비 관객수 증감
    d.audi_change,  -- 전일 대비 관객 증감률(%)
    d.audi_acc, -- 누적 관객수
    d.scrn_cnt, -- 상영 스크린 수
    d.show_cnt, -- 상영 횟수
    -- 기타 영화 속성
    dm.main_genre,  -- 대표 장르
    dm.main_nation, -- 대표 제작 국가
    dm.director_main    -- 대표 감독
FROM {{ ref('stg_daily_boxoffice') }} d
LEFT JOIN {{ ref('dim_movie') }} dm
    ON d.movie_cd = dm.movie_cd

{% if is_incremental() %}
WHERE d.dt > (SELECT max(dt) FROM {{ this }})
{% endif %}