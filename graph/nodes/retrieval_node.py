# Node that retrives columns from a vector database based on a simple question. Gives out hash ids and data of the retrieved columns
from typing import Any, Dict
from graph.state import SubQueryState
from ingestion.retrieval import retriver_object


def retriever_node(state: SubQueryState) -> Dict[str, Any]:
    print("---RETRIEVE---")
    simple_question = state["simple_question"]

    retrieved_docs = retriver_object.invoke(simple_question)
    retrieved_docs_metadata = [doc.metadata for doc in retrieved_docs]
    
    # pick unique elemets with unique hash ids from this list and store in a new list
    # since hash ids are unique identifiers for columns and table, we don't want duplicates
    unique_hashes = set()
    final_metadata = []
    for metadata in retrieved_docs_metadata:
        hash_id = metadata['hash_id']
        if hash_id not in unique_hashes:
            unique_hashes.add(hash_id)
            final_metadata.append(metadata)
    # TODO: add logic to rerank the retrieved columns based on some cross encoder
    # TODO: use hybrid retrieval to get better results
    # go to grade retrieval node
    return {"retrieved_columns_metadata": final_metadata}





