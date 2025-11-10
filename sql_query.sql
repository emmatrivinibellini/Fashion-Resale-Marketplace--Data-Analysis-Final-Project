{\rtf1\ansi\ansicpg1252\cocoartf2709
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww28600\viewh18000\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 -- SECTION 1: Table Creation\
\
\
CREATE TABLE fashion_data (\
    user_uuid TEXT,\
    category TEXT,\
    designer_id TEXT,\
    language TEXT,\
    level TEXT,\
    country TEXT,\
    purchase_date DATE,\
    platform TEXT,\
    item_id TEXT,\
    stars INTEGER,\
    subscription_date DATE\
);\
\
-- SECTION 2: Data Preparation for 2021 Analysis\
CREATE OR REPLACE VIEW fashion_data_2021 AS\
SELECT\
    user_uuid,\
    category,\
    designer_id,\
    language,\
    level,\
    country,\
    purchase_date,\
    TO_CHAR(purchase_date, 'YYYY-MM-DD') AS dt_purchase_date,\
    EXTRACT(YEAR FROM purchase_date)::INT AS purchase_year,\
    EXTRACT(QUARTER FROM purchase_date)::INT AS purchase_quarter,\
    EXTRACT(MONTH FROM purchase_date)::INT AS purchase_month,\
    platform,\
    item_id,\
    stars,\
    subscription_date,\
    TO_CHAR(subscription_date, 'YYYY-MM-DD') AS dt_subscription_date,\
    EXTRACT(YEAR FROM subscription_date)::INT AS subscription_year,\
    EXTRACT(QUARTER FROM subscription_date)::INT AS subscription_quarter\
FROM fashion_data\
WHERE purchase_date BETWEEN '2021-01-01' AND '2021-12-31'\
ORDER BY purchase_date ASC;\
\
-- SECTION 3: Analytical Queries for 2021\
\
-- 1. Sales by 'level' with percentage and ranking\
WITH sales_per_level AS (\
    SELECT\
        level,\
        COUNT(item_id) AS number_of_sales\
    FROM fashion_data_2021\
    GROUP BY level\
)\
SELECT\
    level,\
    number_of_sales,\
    ROUND(100.0 * number_of_sales / SUM(number_of_sales) OVER (), 2) AS percentage_of_total,\
    RANK() OVER (ORDER BY number_of_sales DESC) AS sales_rank\
FROM sales_per_level\
ORDER BY sales_rank;\
\
-- 2. Top 3 countries per 'level' by distinct users\
WITH country_level_users AS (\
    SELECT\
        country,\
        level,\
        COUNT(DISTINCT user_uuid) AS number_of_users\
    FROM fashion_data_2021\
    GROUP BY country, level\
)\
SELECT *\
FROM (\
    SELECT\
        country,\
        level,\
        number_of_users,\
        RANK() OVER (PARTITION BY level ORDER BY number_of_users DESC) AS country_rank\
    FROM country_level_users\
) ranked\
WHERE country_rank <= 3\
ORDER BY level, country_rank;\
\
-- 3. Sales breakdown by platform and category with share per platform\
WITH platform_sales AS (\
    SELECT\
        platform,\
        category,\
        level,\
        COUNT(item_id) AS number_of_sales\
    FROM fashion_data_2021\
    GROUP BY platform, category, level\
)\
SELECT\
    platform,\
    category,\
    level,\
    number_of_sales,\
    ROUND(100.0 * number_of_sales / SUM(number_of_sales) OVER (PARTITION BY platform), 2) AS share_within_platform\
FROM platform_sales\
ORDER BY platform, number_of_sales DESC;\
\
-- 4. Most used platform per 'level' including percentage usage\
WITH platform_level_sales AS (\
    SELECT\
        platform,\
        level,\
        COUNT(item_id) AS number_of_sales\
    FROM fashion_data_2021\
    GROUP BY platform, level\
)\
SELECT\
    platform,\
    level,\
    number_of_sales,\
    ROUND(100.0 * number_of_sales / SUM(number_of_sales) OVER (PARTITION BY level), 2) AS share_within_level\
FROM platform_level_sales\
ORDER BY level, number_of_sales DESC;\
\
-- 5. Top selling designer per category in 2021\
WITH designer_sales AS (\
    SELECT\
        category,\
        designer_id,\
        COUNT(item_id) AS total_sales\
    FROM fashion_data_2021\
    GROUP BY category, designer_id\
)\
SELECT *\
FROM (\
    SELECT\
        category,\
        designer_id,\
        total_sales,\
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_sales DESC) AS rn\
    FROM designer_sales\
) ranked\
WHERE rn = 1\
ORDER BY category;\
\
\
-- SECTION 4: Clustering\
\
-- Calculate user-level metrics: total sales and average stars (rounded to 1 decimal)\
WITH user_metrics AS (\
    SELECT \
        user_uuid,\
        COUNT(*) AS total_sales,  -- Total number of sales per user\
        ROUND(AVG(stars)::numeric, 1) AS avg_stars  -- Average rating (stars) per user, rounded to 1 decimal place\
    FROM fashion_data\
    GROUP BY user_uuid\
),\
\
-- Calculate median values for total_sales and avg_stars across all users\
stats AS (\
    SELECT\
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_sales) AS median_sales,  -- Median of total sales\
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_stars) AS median_stars    -- Median of average stars\
    FROM user_metrics\
),\
\
-- Assign each user to a cluster based on their total_sales and avg_stars relative to the medians\
clustered AS (\
    SELECT \
        um.user_uuid,\
        um.total_sales,\
        um.avg_stars,\
        CASE \
            WHEN um.total_sales < s.median_sales AND um.avg_stars < s.median_stars THEN 'Gabriel'   -- Low sales, low rating\
            WHEN um.total_sales < s.median_sales AND um.avg_stars >= s.median_stars THEN 'Emily'    -- Low sales, high rating\
            WHEN um.total_sales >= s.median_sales AND um.avg_stars < s.median_stars THEN 'Camille'  -- High sales, low rating\
            WHEN um.total_sales >= s.median_sales AND um.avg_stars >= s.median_stars THEN 'Sylvie'  -- High sales, high rating\
        END AS cluster_name\
    FROM user_metrics um\
    CROSS JOIN stats s  -- Combine metrics with median stats for comparison\
)\
\
-- Aggregate results: number of users and percentage per cluster\
SELECT \
    cluster_name,\
    COUNT(*) AS user_count,  -- Total number of users in each cluster\
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_users  -- Percentage of users in each cluster\
FROM clustered\
GROUP BY cluster_name\
ORDER BY cluster_name;\
\
}