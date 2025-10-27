# tools/spark_manager.py
from pyspark.sql import SparkSession

def get_spark():
    # If a SparkSession is already active (e.g., in EMR notebook),
    # just grab the existing one instead of creating a new one
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            # fallback for local dev
            spark = SparkSession.builder.appName("LangGraphSubqueryAgent").getOrCreate()
        return spark
    except Exception as e:
        raise RuntimeError(f"Failed to get or create SparkSession: {e}")
    
