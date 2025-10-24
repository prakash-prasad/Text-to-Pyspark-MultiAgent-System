# This node calls grade retrieval chain to grade the relevance of retrieved columns to the simple question
from typing import Any, Dict
import pandas as pd
from graph.state import SubQueryState
from graph.chains.grade_retrieval_chain import retrieval_grader

relational_db = pd.read_parquet("data/relational_database.parquet")


# TODO: add reranking of rag output as a step here - very important
def grade_retriever_node(state: SubQueryState) -> Dict[str, Any]:
    """
    Grades the relevance of retrieved columns to the simple question using the retrieval_grader chain.
    Args:
        state (SubQueryState): The current state of the subquery agent graph.
    Returns:
    Dict[str, Any]: Updated state with graded columns segments.
    """
    
    print("---GRADE RETRIEVAL---")
    simple_question = state["simple_question"]
    retrieved_columns_metadata = state["retrieved_columns_metadata"]

    graded_columns_segments = {"yes": [], "no": []}
    for column_metadata in retrieved_columns_metadata:
        hash_id = column_metadata['hash_id']
        column_row = relational_db[relational_db['hash_id'] == hash_id]
        # convert the row to a string representation
        column_row = column_row.to_dict(orient='records')[0]

        response = retrieval_grader.invoke(input={
            "question": simple_question, 
            "column_details": column_row
        })
        print(f"Grading response for column {hash_id}: {response}")
        grade = response.binary_score.lower()

        # only take table_name and column name from column_metadata
        # column_metadata_clean = {
        #     "table_name": column_metadata["table_name"],
        #     "column_name": column_metadata["column_name"]
        # }
        column_metadata_clean = column_metadata

        if grade == "yes":
            graded_columns_segments["yes"].append(column_metadata_clean)
        else:
            graded_columns_segments["no"].append(column_metadata_clean)

    # TODO: in yes, pass cross encoder ranked columns in order
    # TODO: give cross encoder ranking to llm as well when deciding yes/no
    # Pass this to human column eval node
    return {"graded_columns_segments": graded_columns_segments}
