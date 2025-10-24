# chain takes query, code and column, gives remarks for improvement, score out of 10 for code quality and output variable names

from os import system
import struct
from typing import List
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

from graph.chains.prompts.code_evaluator_prompt import llm_code_evaluator_prompt

llm = ChatOpenAI(temperature=0.8, model_name="gpt-5", reasoning_effort='high',
                 verbosity='high')

class EvaluatePysparkCode(BaseModel):
    """
    Schema for evaluating PySpark code quality and extracting output variable names.
    """
    remarks: List[str] = Field(
        description=(
            "A list of constructive remarks highlighting the weaknesses of the provided PySpark code. "
            "These remarks should guide improvements and optimizations."
        )
    )
    score: int = Field(
        description=(
            "An overall integer score (out of 10) reflecting the quality, efficiency, and correctness of the provided PySpark code."
        )
    )

    # include this once we are sure of the score 
    # output_variable_names: List[str] = Field(
    #     description=(
    #         "A list of the final, resulting DataFrame or variable names created by the 'code_to_evaluate'. "
    #         "These variables represent the output of the current processing step."
    #     )
    # )


structured_llm_code_evaluator = llm.with_structured_output(EvaluatePysparkCode)

system = llm_code_evaluator_prompt

evaluator_prompt = ChatPromptTemplate.from_messages(
    [
        ("system", system),
        ("human", ": \n\n Relevant coulum details are {required_columns_data} \n\n User question: {question} \n\n Generated code to evaluate: {code_to_evaluate}"),
    ]
)

pyspark_code_evaluator = evaluator_prompt | structured_llm_code_evaluator

