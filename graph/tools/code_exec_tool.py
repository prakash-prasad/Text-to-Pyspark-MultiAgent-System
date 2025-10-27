def safe_exec(code: str, context: dict = None):
    import io, sys
    context = context or {}
    output = io.StringIO()
    sys.stdout = output
    try:
        exec(code, context)
        sys.stdout = sys.__stdout__
        return {"status": "success", "context": context}
    except Exception as e:
        sys.stdout = sys.__stdout__
        return {"status": "failed", "error": str(e)}
    
