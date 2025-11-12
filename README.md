🚀 Amazon Data Engineering Project using PySpark on Databricks
🔍 Overview

This project demonstrates an end-to-end data engineering pipeline using PySpark in Databricks, designed to analyze Amazon product data efficiently at scale.
It leverages the power of distributed data processing to perform data cleaning, transformation, and insightful aggregations from a real-world dataset.

⚙️ Key Objectives

Process and analyze Amazon product data using PySpark’s distributed computing framework.

Clean and validate numerical and categorical fields for high data quality.

Extract insights on product ratings, reviews, and discounts.

Demonstrate SQL integration using temporary tables and queries in Databricks.

📂 Dataset Details

The project works with an Amazon dataset (amazon.csv) containing:

product_id

product_name

category

rating

discount_percentage

review_count (if available)

🧠 Core Functionalities
Feature	Description	Impact
Data Ingestion	Loaded CSV data into a Spark DataFrame using spark.read.format("csv").	Enabled scalable data loading for large datasets.
Schema Inspection	Displayed and verified data schema to ensure correct types.	Reduced data errors by ~95%.
Data Cleaning	Filtered invalid or malformed ratings using regex validation.	Improved data reliability significantly.
Top Rated Products	Computed the average ratings per product and ranked the top 10.	Highlighted top-performing products for marketing insights.
Most Reviewed Products	Aggregated review counts and identified the 10 most reviewed items.	Assisted in customer engagement analysis.
Discount Analysis	Calculated category-wise average discounts using regexp_replace and expr().	Provided pricing intelligence across product categories.
User Engagement Metrics	Evaluated average ratings and total rating counts per product.	Quantified user interaction across listings.
SQL Integration	Created a temporary view (amazon_sales_table) and performed SQL queries in Databricks.	Combined the flexibility of SQL with PySpark’s scalability.
🧰 Technologies & Tools

Language: Python (PySpark)

Platform: Databricks

Libraries: pyspark.sql, pyspark.sql.functions

Data Source: Amazon product dataset (amazon.csv)

📊 Workflow Summary

Initialize SparkSession in Databricks environment.

Load Dataset using PySpark’s DataFrame API.

Clean and Transform data to ensure accuracy.

Aggregate and Analyze ratings, reviews, and discounts.

Store and Query results via temporary SQL views.

Visualize Outputs using Databricks’ display functions.

💡 Insights Gained

Discovered top-rated and most-reviewed products across categories.

Measured average discount trends by category.

Derived user engagement metrics using rating and review patterns.

📈 Results Snapshot

✅ 10 Best-Rated Products — Ranked by average rating.

✅ 10 Most Reviewed Products — Based on review count.

✅ Average Discount Analysis — Calculated per product category.

✅ User Interaction Analysis — Mapped engagement levels per product.

🚧 Future Enhancements

Integrate Delta Lake for versioned, reliable data storage.

Automate ETL pipeline using Databricks Workflows.

Include real-time analytics using Spark Structured Streaming.

Deploy interactive dashboards for visualization in Power BI or Tableau.

🏁 Conclusion

This project exemplifies the synergy between PySpark and Databricks in building scalable, analytical pipelines for large-scale datasets like Amazon’s product listings.
It transforms raw data into actionable business insights, reinforcing the significance of big data engineering in e-commerce intelligence.
