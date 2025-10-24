# Currently takes grade retrival output and asks human for changes
# Human may ask to send some columns from no to yes, or some columns from yes to no
# default behaviour is to keep things as is.

from typing import Any, Dict
from graph.state import SubQueryState

def human_column_eval_node(state: SubQueryState) -> Dict[str, Any]:
    """
    Allows human evaluation of graded columns segments.
    Args:
        state (SubQueryState): The current state of the subquery agent graph.
    Returns:
        Dict[str, Any]: Updated state with potentially modified graded columns segments.
    """
    print("---HUMAN COLUMN EVALUATION---")
    graded_columns_segments = state["graded_columns_segments"]

    # For simplicity, we will just print the current segments and assume human approves them.
    print("Current graded columns segments:")
    print("YES segment:")
    for col in graded_columns_segments["yes"]:
        print(col)
    print("NO segment:")
    for col in graded_columns_segments["no"]:
        print(col)

    # In a real scenario, you would collect human input here to modify the segments.
    # For now, we will return the segments as is.

    # human_evaluation_input = input("Do you want to modify the segments? (y/n): ")
    human_evaluation_input = 'y'
    if human_evaluation_input.lower() == "y":
        # TODO: Implement human modification logic
        # Step 1: ask human input for columns to move from yes to no
        # Step 2: give this to llm to process and give hash ids to move from yes to no and no to yes
        # Step 3: update graded_columns_segments accordingly using an explicit python code

        # Step 4: In phase 2, ask human to modify the simple query itself for better retrieval
        print("Human modification not implemented in this mockup. Keeping original segments.")
        human_column_evaluation = True
    else:
        print("Keeping original segments.")
        human_column_evaluation = False
    
    # If human_column_evaluation is yes pass to pyspark code generator node else go to retiveal for and run modified simple question
    return {"simple_question": state["simple_question"],
            "graded_columns_segments": graded_columns_segments,
            "human_column_evaluation": human_column_evaluation}
