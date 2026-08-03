"""
Gold -> ML: unsupervised customer segmentation with K-Means.

This is deliberately a *different* segmentation from the rule-based RFM
segment in gold/03_silver_to_gold_rfm.py, and that contrast is the
teaching point: RFM segments are fixed business rules anyone can read and
argue with; K-Means finds groupings from the data's own structure, adding
behavioral features (session frequency, cart abandonment) that a simple
RFM rule doesn't consider. In a real deployment you'd show both to
stakeholders and reconcile the two.

Run with: python 05_customer_segmentation_kmeans.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_spark, SILVER_PATH, GOLD_PATH
from pyspark.sql import functions as F, Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

FEATURE_COLS = [
    "recency_days", "frequency", "monetary",
    "num_sessions", "avg_session_duration_secs", "cart_abandonment_rate",
]


def build_feature_table(spark):
    customer_360 = spark.read.format("delta").load(f"{GOLD_PATH}/customer_360")
    sessions = spark.read.format("delta").load(f"{SILVER_PATH}/sessions")

    session_features = sessions.groupBy("user_id").agg(
        F.count("*").alias("num_sessions"),
        F.avg("session_duration_secs").alias("avg_session_duration_secs"),
        F.avg(F.when(F.col("funnel_stage_reached") == "cart_abandoned", 1.0).otherwise(0.0)).alias(
            "cart_abandonment_rate"
        ),
    )

    # only customers with at least one completed order - clustering "never
    # purchased" visitors together with buyers would just rediscover
    # "bought something vs. didn't", which RFM already tells us for free.
    features = (
        customer_360.filter(F.col("frequency") > 0)
        .join(session_features, "user_id", "left")
        .fillna(0.0, subset=["avg_session_duration_secs", "cart_abandonment_rate"])
    )
    return features


def pick_k_with_elbow(spark, scaled_df, k_range=range(2, 7)):
    """Print inertia (WSSSE) and silhouette per k - what you'd show the
    class to justify the chosen k rather than picking it out of thin air."""
    print("Elbow method - within-cluster sum of squared errors per k:")
    for k in k_range:
        km = KMeans(featuresCol="scaled_features", k=k, seed=42)
        model = km.fit(scaled_df)
        wssse = model.summary.trainingCost
        predictions = model.transform(scaled_df)
        evaluator = ClusteringEvaluator(featuresCol="scaled_features")
        silhouette = evaluator.evaluate(predictions)
        print(f"  k={k}: WSSSE={wssse:,.1f}  silhouette={silhouette:.3f}")


def run_kmeans(spark, features, k=4):
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
    assembled = assembler.transform(features)

    scaler = StandardScaler(
        inputCol="raw_features", outputCol="scaled_features", withMean=True, withStd=True
    )
    scaled = scaler.fit(assembled).transform(assembled)

    pick_k_with_elbow(spark, scaled)

    kmeans = KMeans(featuresCol="scaled_features", predictionCol="cluster", k=k, seed=42)
    model = kmeans.fit(scaled)
    clustered = model.transform(scaled)

    # Rank clusters by average monetary value so labels mean something
    # regardless of the arbitrary cluster ids K-Means assigns.
    cluster_ranks = (
        clustered.groupBy("cluster")
        .agg(F.avg("monetary").alias("avg_monetary"), F.avg("frequency").alias("avg_frequency"))
        .orderBy(F.col("avg_monetary").desc())
        .withColumn("rank", F.row_number().over(Window.orderBy(F.col("avg_monetary").desc())))
    )

    labels = ["High-Value Loyalists", "Growing Spenders", "Occasional Buyers", "Low-Engagement / Price-Sensitive"]
    label_col = F.array(*[F.lit(l) for l in labels])
    cluster_ranks = cluster_ranks.withColumn("segment_label", label_col[F.col("rank") - 1])

    print("\nCluster profile (ranked by average monetary value):")
    cluster_ranks.show(truncate=False)

    result = (
        clustered.join(cluster_ranks.select("cluster", "segment_label"), "cluster")
        .select("user_id", "cluster", "segment_label", *FEATURE_COLS)
    )

    out = f"{GOLD_PATH}/customer_segments_kmeans"
    result.write.format("delta").mode("overwrite").save(out)
    print(f"\ncustomer_segments_kmeans: {result.count():,} rows -> {out}")

    print("\nSegment sizes:")
    result.groupBy("segment_label").count().orderBy(F.col("count").desc()).show()

    return result


if __name__ == "__main__":
    spark = get_spark("EcomProject_KMeansSegmentation")
    features = build_feature_table(spark)
    run_kmeans(spark, features, k=4)
    spark.stop()
