-- 순위/매출/관객 데이터를 주차 단위로 분석
WITH weekly AS (
    SELECT
        year_week_time,
        movie_cd,
        movie_nm,
        start_range AS week_start_dt,
        end_range   AS week_end_dt,
        DATEADD(day, 3, start_range) AS week_center_dt,
        rank,
        rank_inten,
        rank_old_and_new,
        sales_amt,
        sales_share,
        sales_inten,
        sales_change,
        sales_acc,
        audi_cnt,
        audi_inten,
        audi_change,
        audi_acc,
        scrn_cnt,
        show_cnt
    FROM {{ ref('stg_weekly_boxoffice') }}
)
SELECT
    -- PK
    w.year_week_time,   -- 주차 기준 키
    w.movie_cd, -- 영화 코드(FK)
    -- 이름 정보
    coalesce(w.movie_nm, dm.movie_nm) as movie_nm,  -- 영화명
    -- 기간 정보
    w.week_start_dt,    -- 주간 시작일
    w.week_end_dt,  -- 주간 종료일
    w.week_center_dt,   -- 주간 대표 날짜(가운데 날짜)
    -- 박스오피스 지표
    rank,   -- 주간 박스오피스 순위
    rank_inten, -- 전주 대비 순위 증감
    rank_old_and_new,   -- 신규 진입 여부 (NEW/OLD)
    sales_amt,  -- 매출액
    sales_share,    -- 전체 매출 대비 비율(%)
    sales_inten,    -- 매출 증감액(전주 대비)
    sales_change,   -- 매출 증감률(전주 대비 %)
    sales_acc,  -- 누적 매출액
    audi_cnt,   -- 주간 관객수
    audi_inten, -- 관객 증감(전주 대비)
    audi_change,    -- 관객 증감률(전주 대비 %)
    audi_acc,   -- 누적 관객수
    scrn_cnt,   -- 스크린 수
    show_cnt,   -- 상영 횟수
    -- 기타 영화 속성
    dm.genre_nm,  -- 대표 장르
    dm.nation_nm -- 대표 제작 국가
FROM weekly w
LEFT JOIN {{ ref('dim_movie') }} dm
    on w.movie_cd = dm.movie_cd

{% if is_incremental() %}
WHERE w.year_week_time > (
    SELECT max(year_week_time) FROM {{ this }}
)
{% endif %}