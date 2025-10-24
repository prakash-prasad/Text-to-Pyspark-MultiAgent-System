# chain that takes retrieved data and tells if it is relevant to the question or not

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from graph.chains.prompts.grade_retrieval_prompt import grade_retrieval_system_prompt
llm = ChatOpenAI(temperature=0, model_name="gpt-5-nano")


class GradeColumns(BaseModel):
    """Binary score for relevance check on retrieved Columns."""

    binary_score: str = Field(
        description="Columns and tables are relevant to the question, 'yes' or 'no'"
    )

structured_llm_grader = llm.with_structured_output(GradeColumns)

system = grade_retrieval_system_prompt


grade_prompt = ChatPromptTemplate.from_messages(
    [
        ("system", system),
        ("human", "Retrieved column details: \n\n {column_details} \n\n User question: {question}"),
    ]
)


retrieval_grader = grade_prompt | structured_llm_grader
