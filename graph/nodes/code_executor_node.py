# Executes last generated code using tools like exec

from turtle import st
from typing import Any, Dict, List, Optional
from graph.state import SubQueryState
from graph.tools.spark_manager import get_spark

def pyspark_code_executor(state: SubQueryState) -> Any:
    """
    Executes the provided PySpark code in the context of the current SubQueryState.
    
    Args:
        state (SubQueryState): The current state of the subquery agent graph.
        
    Returns:
        Any: The result of the executed code.
    """
    print("---PYSPARK CODE EXECUTION---")
    
    # Get code to execute from state object
    pyspark_code = state["pyspark_code_raw"]
    spark = get_spark()
    local_env = {"spark": spark}
    
    try:
        result = eval(pyspark_code, {}, local_env)
    except SyntaxError:
        exec(pyspark_code, {}, local_env)
        result = None
    except Exception as e:
        result = str(e)

    # TODO: make sure the output (results is in a list format)
    output_variable_list = state["output_variable_names_raw"]
    code_execution_results_dict = state.get("code_execution_results", {})
    if output_variable_list and len(output_variable_list) <= len(result):
        for index, variable_name in enumerate(output_variable_list):
            code_execution_results_dict[variable_name] = result[index]
            
    return {"code_execution_results": code_execution_results_dict}
