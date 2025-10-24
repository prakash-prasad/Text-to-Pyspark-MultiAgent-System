# Read chroma db from disk, make a retriever object
import os
from typing import Callable, List, Optional, Tuple
from const import RETRIVER_TOP_K
from langchain_community.vectorstores import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document



DB_PATH: str = "chroma_db/metadata_huggingface"
HF_MODEL_NAME: str = "sentence-transformers/all-MiniLM-L6-v2"
HF_CACHE_FOLDER: str = "./models"

embeddings_model = HuggingFaceEmbeddings(model_name=HF_MODEL_NAME, 
                                         cache_folder=HF_CACHE_FOLDER)

vector_database = Chroma(persist_directory=DB_PATH, 
                  embedding_function=embeddings_model
                )

# Get the top K most relevant documents
retriver_object = vector_database.as_retriever(search_type="similarity",
                    search_kwargs={"k": RETRIVER_TOP_K}  
                    )


def get_distinct_metadata_pairs(docs: List[Document]) -> set[Tuple[str, str]]:
    """Helper function to extract unique (table_name, column_name) pairs."""
    unique_pairs = set()
    for doc in docs:
        metadata = doc.metadata
        table = metadata.get('table_name')
        column = metadata.get('column_name')
        # Only add if both values exist
        if table and column:
            unique_pairs.add((table, column))
    return unique_pairs


def iterative_retriever_by_metadata(
    vector_database: Chroma, 
    query: str, 
    target_pairs: int = 3, 
    k_initial: int = 5, 
    k_increment: int = 5,
    max_total_docs: int = 50) -> List[Document]:
    """
    Iteratively retrieves documents until the target number of unique 
    (table_name, column_name) pairs is reached.

    Args:
        vector_database: The initialized LangChain VectorStore object (e.g., Chroma).
        query (str): The search query string.
        target_pairs (int): The number of distinct (table, column) pairs required.
        k_initial (int): The starting number of documents to retrieve.
        k_increment (int): The amount to increase 'k' by in each subsequent iteration.
        max_total_docs (int): Safety limit to prevent excessive/infinite retrieval.

    Returns:
        List[Document]: The final list of documents.
    """
    
    current_k = k_initial
    
    while True:
        # 1. Retrieve the current top K documents
        # The Chroma.similarity_search() method is called directly, allowing us 
        # to dynamically change the 'k' parameter.
        all_retrieved_docs = vector_database.similarity_search(query, k=current_k)
        
        # 2. Check the diversity condition
        unique_pairs = get_distinct_metadata_pairs(all_retrieved_docs)
        
        # print(f"Iteration k={current_k}: Found {len(unique_pairs)} distinct pairs.")
        
        if len(unique_pairs) >= target_pairs:
            # Condition met! Stop and return the documents.
            # print(f"Target of {target_pairs} unique pairs reached. Stopping.")
            return all_retrieved_docs
            
        # 3. Check safety break condition (if we hit the limit)
        if current_k >= max_total_docs:
            # print(f"Safety limit of {max_total_docs} documents reached. Stopping retrieval.")
            return all_retrieved_docs

        # 4. Increase k for the next iteration
        current_k += k_increment


def create_iterative_retriever_runnable(
    vector_database: Chroma, 
    target_pairs: int = 3) -> Callable[[str], List[Document]]:
    """
    Creates an LCEL-compatible Runnable component using a function closure.
    
    Args:
        vector_database: The initialized LangChain VectorStore object (e.g., Chroma).
        target_pairs (int): The number of distinct (table, column) pairs required.

    Returns:
        A function (Runnable) that accepts a query string and returns a List[Document].
    """
    
    def runnable_retriever(query: str) -> List[Document]:
        """Wrapper function compatible with LCEL piping."""
        return iterative_retriever_by_metadata(
            vector_database=vector_database,
            query=query,
            target_pairs=target_pairs,
            k_initial=5,
            k_increment=5,
            max_total_docs=50
        )

    # Note: When using a standard function in an LCEL chain, it is already treated as a Runnable.
    # We can also wrap it in RunnableLambda if we want to provide an explicit name or config.
    # return RunnableLambda(runnable_retriever, name="IterativeRetriever")
    return runnable_retriever