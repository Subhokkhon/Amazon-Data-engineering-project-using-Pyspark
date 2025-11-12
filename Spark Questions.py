Top Products by Rating:
python
Copy code
top_rated_products = df.groupBy('product_id', 'product_name').agg(avg('rating').alias('avg_rating')).orderBy(desc('avg_rating')).limit(10)
Most Reviewed Products:
python
Copy code
most_reviewed_products = df.groupBy('product_id', 'product_name').count().orderBy(desc('count')).limit(10)
Discount Analysis:
python
Copy code
discount_analysis = df.groupBy('category').agg(avg('discount_percentage').alias('avg_discount'))
Review Length Analysis:
python
Copy code
review_length_analysis = df.withColumn('review_length', length('review_content')).agg(avg('review_length').alias('avg_review_length'))
Sentiment Analysis:
python
Copy code
# Assuming you have a sentiment analysis function called analyze_sentiment
sentiment_analysis = df.withColumn('sentiment', analyze_sentiment('review_content'))
User Engagement:
python
Copy code
user_engagement = df.groupBy('product_id').agg(avg('rating').alias('avg_rating'), count('rating').alias('rating_count'))
Price Range Analysis:
python
Copy code
price_range_analysis = df.withColumn('price_range', when(col('discounted_price') <= 50, 'Low').when(col('discounted_price') <= 100, 'Medium').otherwise('High')).groupBy('price_range').count()
Category-wise Analysis:
python
Copy code
category_analysis = df.groupBy('category').agg(avg('rating').alias('avg_rating'), count('rating').alias('rating_count'), avg('discount_percentage').alias('avg_discount'))
User Review Patterns:
python
Copy code
user_review_patterns = df.groupBy('user_id', 'user_name').count().orderBy(desc('count')).limit(10)
Product Recommendations:
python
Copy code
# Assuming you have a recommendation function called recommend_products
recommended_products = recommend_products(df)