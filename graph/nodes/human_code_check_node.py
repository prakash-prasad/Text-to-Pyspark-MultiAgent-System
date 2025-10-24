# Allows human to review the generated code and provide feedback

from importlib import simple
from typing import Any, Dict, List
from graph.state import SubQueryState
from graph.chains.pyspark_code_generator_chain import variable_name_extractor

def human_code_check_node(state: SubQueryState) -> Dict[str, Any]:
    """
    Allows human review of generated PySpark code.
    Args:
        state (SubQueryState): The current state of the subquery agent graph.
    Returns:
        Dict[str, Any]: Updated state with human evaluation feedback.
    """
    print("---HUMAN CODE REVIEW---")
    pyspark_code_history = state["pyspark_code_raw"]
    latest_pyspark_code = pyspark_code_history[-1] if pyspark_code_history else ""

    # get latest simple question
    simple_query = state["simple_question"]
    print(f"Generated PySpark Code for query {simple_query}:")
    print(latest_pyspark_code)

    # In a real scenario, you would collect human input here to evaluate the code.
    # For now, we will simulate human approval.

    human_code_approval = input("Do you approve this code? (y/n): ")
    # human_code_approval = 'y'
    if human_code_approval.lower() == "y":
        print("Human approved the code.")
        human_code_evaluation = True
        # update output_variable_names_raw in state to get names of output variables the code is supposed to generate
        response = variable_name_extractor.invoke(input={"pyspark_code": latest_pyspark_code, "question": simple_query})
        existing_output_variable_names: List[str] = response.output_variable_names
    else:
        # if human dosent approve, ask for feedback and update list of evaluation remarks for next generation
        human_feedback = input("Please provide your feedback for improvement: ")
        human_code_evaluation = False
        existing_output_variable_names = []

    # if human approves, go to code executor node else go to code generator node for re-generation
    return {
        "human_code_evaluation": human_code_evaluation,
        "code_evaluation_remarks": state["code_evaluation_remarks"] + [[human_feedback]] if not human_code_evaluation else state["code_evaluation_remarks"],
        "output_variable_names_raw": existing_output_variable_names
    }

