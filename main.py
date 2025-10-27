# main.py
from graph.tools.spark_manager import get_spark
from agents.subquery_agent.graph import subquery_graph

def main():
    spark = get_spark()
    print(f"✅ SparkSession connected — {spark.sparkContext.master}")

    initial_state = {
        "user_query": "Find top 5 customers by sales",
        "pyspark_code_raw": None,
        "debugger_results_list": [],
        "execution_status": "pending"
    }

    final_state = subquery_graph.invoke(initial_state)
    print(final_state)

if __name__ == "__main__":
    main()
