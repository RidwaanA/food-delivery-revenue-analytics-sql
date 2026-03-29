# Revenue Analytics Data Pipeline for a Multi-City Food Delivery Platform

# Project Overview

Designed a MySQL-based analytics pipeline to support revenue tracking and performance analysis for a food delivery platform operating across 98 cities in 14 countries.

The solution enables standardized revenue computation, weekly KPI tracking, and business-ready insights for cross-functional teams.

# Business Context
Great Eats operates a food delivery platform across ~100 cities in 14 countries. Following a revenue decline, stakeholders across Marketing, Operations, and Finance required a centralized data system to analyze performance trends and identify root causes.

# Data Overview
- 130K+ orders, 13K+ customers, 899 restaurants
- Coverage across 98 cities / 14 countries
- Key data points: orders, customers, delivery performance, pricing, and ratings

# Tools & Technologies
- MySQL
- SQL (CTEs, window functions, aggregations, CASE logic)
- Custom revenue function: calcRevenue()

# SQL Highlights
1. Week-over-Week Revenue Trend

with WkoWk as (
	select
		WEEK_NUMBER,
        sum(calcRevenue(ORDER_COST, DISCOUNT, DELIVERY_STATUS)) as REVENUE
	from gl_eats_del_ord_v
	group by 1
    )
select
	WEEK_NUMBER,
    REVENUE,
    lag(REVENUE) over(order by WEEK_NUMBER) as PREVIOUS_REVENUE,
    ((REVENUE-lag(REVENUE) over(order by WEEK_NUMBER)) / lag(REVENUE) over(order by WEEK_NUMBER) * 100) as 'WEEK_OVER_WEEK_REVENUE(%)'
from WkoWk;

2. Customer Cancellation Rate

select
	CUSTOMER_ID,
    count(*) as TOTAL_ORDERS,
    sum(case when DELIVERY_STATUS = 4 then 1 else 0 end) as CANCELLATIONS,
    round(sum(case when DELIVERY_STATUS = 4 then 1 else 0 end) / count(*), 2)*100 as CANCELLATION_RATE_PERCENT
from gl_eats_del_ord_v
group by 1
order by 4 desc;

3. Discount vs Customer Rating Analysis

with CustDiscRating as (
	select
		CUSTOMER_ID,
        ORDER_ID,
        case
			when DISCOUNT >= 15 and DISCOUNT <= 18 then 'LOW'
            when DISCOUNT >= 19 and DISCOUNT <= 21 then 'Medium'
            when DISCOUNT >=22 then 'HIGH'
		end as DISCOUNT_BUCKET,
        case
			when STAR_RATING = 1 or STAR_RATING = 2 then 'LOW'
            when STAR_RATING = 3 or STAR_RATING = 4 then 'MEDIUM'
            when STAR_RATING = 5 then 'HIGH'
		end as RATING_BUCKET
	from gl_eats_del_ord_v
    )
select
	DISCOUNT_BUCKET,
    RATING_BUCKET,
    count(RATING_BUCKET) as CNT,
    count(RATING_BUCKET) / (select count(*) from CustDiscRating where DISCOUNT_BUCKET = 'HIGH') as RATIO
from CustDiscRating
where DISCOUNT_BUCKET = 'HIGH'
group by 1,2;

# Key Insights
- Revenue declined consistently week-over-week (₦5.22M → ₦2.19M), despite highest activity in Week 1
- Order volume followed the same downward trend, indicating demand drop rather than pricing fluctuation
- Average order cost remained stable (~124), confirming revenue decline was volume-driven
- ~17% of customers cancel orders, with certain cities exceeding 10% cancellation rates
- Discounts do not guarantee satisfaction — only ~50% of high-discount orders received high ratings
- Customer satisfaction is generally strong, with 80%+ ratings above 3.5, but ~4% remain dissatisfied
- Top revenue concentrated geographically — a few countries and cities drive performance
- Restaurant rating has minimal impact on cancellations (~5% across tiers), suggesting operational issues instead
- Workforce imbalance identified — a small group of delivery agents (14) are overutilized

# Recommendations
- Investigate and address drivers of declining order volume (retention, competition, seasonality)
- Prioritize cancellation reduction, especially in high-risk cities
- Shift from blanket discounts to targeted incentives
- Focus growth efforts on top-performing markets while fixing underperforming locations
- Implement city-level performance monitoring dashboards

# Outcome

Delivered a scalable SQL analytics solution that uncovered key revenue drivers, identified operational inefficiencies, and provided **actionable insights to support business recovery.**

# Next Steps
- Build dashboards (Tableau/Power BI)
- Add customer segmentation
- Develop demand forecasting models
