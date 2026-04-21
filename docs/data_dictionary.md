# Data Dictionary Template

Use this file to document every important field in your dataset. A strong data dictionary makes your cleaning decisions, KPI logic, and dashboard filters much easier to review.

## How To Use This File

1. Add one row for each column used in analysis or dashboarding.
2. Explain what the field means in plain language.
3. Mention any cleaning or standardization applied.
4. Flag nullable columns, derived fields, and known quality issues.

## Dataset Summary

| Item | Details |
|---|---|
| Dataset name | Olist E-Commerce Business Insights |
| Source | Olist Public Dataset (Brazilian E-Commerce Platform) |
| Raw file names | olist_orders_dataset.csv, olist_order_items_dataset.csv, olist_customers_dataset.csv, olist_order_reviews_dataset.csv, olist_products_dataset.csv, olist_sellers_dataset.csv, olist_geolocation_dataset.csv, product_category_name_translation.csv |
| Last updated | April 2026 |
| Granularity | One row per order (olist_master_dataset.csv); one row per customer (olist_rfm_segments.csv); one row per state (olist_geospatial_summary.csv) |

## Column Definitions

### Order Core Fields

| Column Name | Data Type | Description | Example Value | Used In | Cleaning Notes |
|---|---|---|---|---|---|
| order_id | string | Unique identifier for each order | e481f51cbdc54678b7cc49136f2d6af7 | EDA / KPI / Tableau | Unique primary key; no duplicates |
| order_status | string | Final status of the order (delivered only; cancelled/unavailable removed) | delivered | EDA / Tableau | Filtered to remove cancelled and unavailable orders |
| order_purchase_timestamp | datetime | Date and time when customer placed the order | 2017-10-02 10:56:33 | EDA / KPI / Tableau | Converted to datetime; used for time-series analysis |
| order_approved_at | datetime | Date and time when order was approved by seller | 2017-10-02 11:07:15 | EDA | Converted to datetime |
| order_delivered_carrier_date | datetime | Date when order handed to logistics carrier | 2017-10-04 19:55:00 | EDA | Converted to datetime |
| order_delivered_customer_date | datetime | Date and time when customer received the order | 2017-10-10 21:25:13 | EDA / KPI / Tableau | Converted to datetime; used for delivery time calculation |
| order_estimated_delivery_date | datetime | Estimated delivery date provided at purchase | 2017-10-18 00:00:00 | EDA / KPI | Converted to datetime; used for late delivery detection |
| order_year_month | string | Year-month period of purchase for time-series grouping | 2017-10 | Tableau | Derived from order_purchase_timestamp (YYYY-MM format) |
| order_year | int | Calendar year of purchase | 2017 | Tableau | Extracted from order_purchase_timestamp |

### Customer Fields

| Column Name | Data Type | Description | Example Value | Used In | Cleaning Notes |
|---|---|---|---|---|---|
| customer_id | string | Unique identifier linking to customer record | 9ef432eb6251297304e76186b10a928d | EDA / KPI | Hash-based identifier from original dataset |
| customer_unique_id | string | Unique customer identifier across all orders | 9ef432eb6251297304e76186b10a928d | EDA / KPI / RFM Analysis | Used for RFM segmentation; identifies repeat customers |
| customer_zip_code_prefix | string | First 5 digits of customer ZIP code (Brazilian CEP) | 14409 | Geospatial / Tableau | Standardized to string; used for location mapping |
| customer_city | string | City where customer is located (lowercase) | sao paulo | Geospatial / Tableau | Standardized to lowercase; leading/trailing spaces removed |
| customer_state | string | Brazilian state abbreviation (2-letter code) | SP | Geospatial / KPI / Tableau | Uppercase; used for state-level aggregation |

### Product Fields

| Column Name | Data Type | Description | Example Value | Used In | Cleaning Notes |
|---|---|---|---|---|---|
| product_id | string | Unique identifier for the product | 4244733e06e7ecb4970a6e2683c13e61 | EDA / KPI | Hash-based identifier; links to order items |
| product_category_name_english | string | English product category (translated from Portuguese) | housewares | EDA / KPI / Tableau | Translated from product_category_name using translation table; Unknown for missing |
| product_name_lenght | int | Number of characters in product name | 40 | EDA | Standardized from original field (note: typo 'lenght' preserved from source) |
| product_description_lenght | int | Number of characters in product description | 287 | EDA | Standardized from original field |
| product_photos_qty | int | Number of product photos available | 1 | EDA | Count of photos in product listing |
| product_weight_g | float | Product weight in grams | 225 | EDA | Physical weight measurement |
| product_length_cm | float | Product length in centimeters | 16 | EDA | Physical dimension |
| product_height_cm | float | Product height in centimeters | 10 | EDA | Physical dimension |
| product_width_cm | float | Product width in centimeters | 14 | EDA | Physical dimension |

### Seller Fields

| Column Name | Data Type | Description | Example Value | Used In | Cleaning Notes |
|---|---|---|---|---|---|
| seller_id | string | Unique identifier for the seller | 48436dade18ac8b2bce089ec2a041202 | EDA / KPI | Hash-based identifier; sellers can have multiple orders |
| seller_zip_code_prefix | string | First 5 digits of seller ZIP code (Brazilian CEP) | 13023 | Geospatial | Standardized to string |
| seller_city | string | City where seller is located (lowercase) | campinas | Geospatial / Tableau | Standardized to lowercase |
| seller_state | string | Brazilian state abbreviation for seller (2-letter code) | SP | Geospatial / Tableau | Uppercase; used for seller location analysis |

### Order Item & Payment Fields

| Column Name | Data Type | Description | Example Value | Used In | Cleaning Notes |
|---|---|---|---|---|---|
| order_item_count | int | Total number of line items in the order | 2 | EDA / KPI | Aggregated from olist_order_items_dataset; one row per order |
| total_price | float | Sum of all item prices in order (Brazilian Real) | 89.90 | EDA / KPI / Tableau | Aggregated from individual item prices; also used as 'revenue' |
| total_freight | float | Sum of all freight charges in order (Brazilian Real) | 14.50 | EDA / KPI / Tableau | Shipping costs aggregated per order |
| revenue | float | Alias for total_price used in Tableau dashboards | 89.90 | Tableau | Direct copy of total_price for dashboard visualization |

### Review Fields

| Column Name | Data Type | Description | Example Value | Used In | Cleaning Notes |
|---|---|---|---|---|---|
| review_score | float | Customer satisfaction rating (1-5 stars) | 4.0 | EDA / KPI / Tableau | Numeric score; NaN filled with 0 for aggregations |
| review_comment_message | string | Customer's written review comment | "Great product, fast delivery" | EDA | Text field; filled with 'No Comment' if null for analysis |

### Delivery Performance Fields

| Column Name | Data Type | Description | Example Value | Used In | Cleaning Notes |
|---|---|---|---|---|---|
| delivery_time_days | float | Days from order purchase to actual customer delivery | 8 | EDA / KPI / Tableau | Calculated as (order_delivered_customer_date - order_purchase_timestamp).days |
| delivery_delay_days | float | Difference between actual and estimated delivery in days (positive = late) | 3 | EDA / KPI / Tableau | Calculated as (order_delivered_customer_date - order_estimated_delivery_date).days |
| is_late | boolean | Flag indicating whether order arrived after estimated delivery date | False | EDA / KPI / Tableau | True if delivery_delay_days > 0; used for on-time metrics |

## Derived Columns

### Delivery & Performance Metrics (from olist_master_dataset.csv)

| Derived Column | Logic | Business Meaning |
|---|---|---|
| delivery_time_days | (order_delivered_customer_date - order_purchase_timestamp).dt.days | Total time customer waited for delivery; KPI for operational efficiency |
| delivery_delay_days | (order_delivered_customer_date - order_estimated_delivery_date).dt.days | Service level compliance; negative = early, positive = late |
| is_late | delivery_delay_days > 0 | Boolean flag for late deliveries; impacts CSAT and repeat purchase likelihood |
| order_year_month | order_purchase_timestamp.dt.to_period('M').astype(str) | Period grouping for time-series Tableau visualizations (format: YYYY-MM) |
| order_year | order_purchase_timestamp.dt.year | Annual aggregation for year-over-year analysis and dashboards |

### RFM Segmentation Fields (from olist_rfm_segments.csv)

| Derived Column | Logic | Business Meaning |
|---|---|---|
| recency | days since last purchase | Measure of customer engagement; lower = more recent = more active |
| frequency | total number of purchases | Customer loyalty indicator; higher = more repeat purchases |
| monetary | total lifetime spend in Brazilian Real | Customer value indicator; indicates purchasing power and basket size |
| R | recency score (1-5) | Recency rank quartile; 5 = most recent purchases |
| F | frequency score (1-5) | Frequency rank quartile; 5 = most frequent purchasers |
| M | monetary score (1-5) | Monetary rank quartile; 5 = highest spenders |
| RFM_Score | Concatenation of R + F + M (e.g., "555") | Combined RFM score for quick segment identification |
| RFM_Total | R + F + M (numeric sum) | Total RFM score; used for hierarchical customer ranking |
| Segment | Segmentation logic based on RFM scores | Customer lifecycle segment for targeted marketing (see segment list below) |

### Geospatial Aggregation Fields (from olist_geospatial_summary.csv)

| Derived Column | Logic | Business Meaning |
|---|---|---|
| total_orders | COUNT(order_id) per state | Order volume by region; identifies high-traffic states |
| avg_delivery_time_days | AVG(delivery_time_days) per state | Regional delivery performance baseline |
| avg_delivery_delay_days | AVG(delivery_delay_days) per state | Regional logistics efficiency; indicates logistics network gaps |
| late_orders | SUM(is_late) per state | Count of delayed deliveries by region; identifies problem areas |
| total_revenue | SUM(revenue) per state | Regional revenue contribution; for geographic revenue analysis |
| avg_review_score | AVG(review_score) per state | Regional customer satisfaction; indicates service quality by location |
| on_time_rate | (total_orders - late_orders) / total_orders | Percentage of on-time deliveries per state; key SLA metric |

### RFM Segmentation Summary (from olist_rfm_summary.csv)

| Segment Name | RFM Pattern | Business Strategy |
|---|---|---|
| Champions | High R, High F, High M | VIP customers; most valuable; reward loyalty, exclusive offers |
| Loyal Customers | High F, High M | Core customer base; repeat buyers; cross-sell opportunities |
| Potential Loyalists | Recent purchase, Low-Med F | New/recent buyers; nurture via engagement campaigns |
| Needs Attention | Medium R, Low-Med F, Low-Med M | Declining engagement; re-engagement campaigns needed |
| At Risk | Medium-High R, Low F | Former active customers; losing interest; win-back campaigns |
| High Risk / Churn | High R, Low F, Low M | Dormant customers; churn prevention priority; special offers |
| Lost | Very High R, Low F | Inactive long-term; lowest priority; archive or seasonal reactivation |

## Data Quality Notes

- **Cancelled & Unavailable Orders Removed**: The raw dataset includes orders with statuses 'cancelled' and 'unavailable'. These are filtered out in the cleaning pipeline; only 'delivered' orders remain in the master dataset for accurate revenue and delivery analysis.
- **Portuguese to English Translation**: Product categories are translated from Portuguese using the `product_category_name_translation.csv` lookup table. Missing or unmapped categories are filled with 'Unknown'. Examples: perfumaria → housewares, beleza_saude → health_beauty.
- **Null Handling**: Review comments with missing values are filled with "No Comment" to enable consistent text analysis. Review scores with null values are handled separately in aggregations (average excludes nulls; counts include them as zero for on-time metrics).
- **Identifier Obfuscation**: All customer_id, product_id, and seller_id fields are hashed for privacy; they are not human-readable but remain unique and consistent across datasets for merging.
- **Brazilian Geography**: customer_state and seller_state use 2-letter Brazilian state codes (SP, RJ, BA, MG, etc.). ZIP code prefix fields (first 5 digits) are string type and have leading zeros in some cases (e.g., "01037").
- **Datetime Precision**: Timestamps from order_purchase_timestamp through order_delivered_customer_date are precise to the minute (YYYY-MM-DD HH:MM:SS). Estimated delivery dates are typically midnight (00:00:00) and are used for SLA calculations.
- **Dimension Measurements**: Product dimensions (length, height, width in cm and weight in grams) come from product registry; some may be approximations or defaults. Zero or very small values may indicate missing data.
- **Delivery Time Calculations**: Negative delivery_delay_days indicate early arrival. Null values in delivery_time_days occur when order_delivered_customer_date is missing (rare in filtered dataset). Orders with null delivery_delay_days are excluded from on-time metrics.
- **Time Period Coverage**: Raw data spans from 2016 to 2018; majority of orders are from 2017-2018. Earlier records in 2016 are sparse.
- **Single Item Per Order Aggregation**: In the cleaning pipeline, order-items are aggregated by taking the highest-priced item's product details (first product when sorted by price descending). Multi-item orders are represented as single rows with aggregate totals in total_price and total_freight.
- **Missing Geolocation Data**: geolocation_dataset provides latitude/longitude for ZIP codes but is not directly merged into master dataset; primarily used for mapping visualization reference.
- **RFM Recency Baseline**: Recency is calculated as days from last purchase to the dataset's maximum date (reference point is the last order date in the dataset). This allows consistent segment classification across analysis runs.
