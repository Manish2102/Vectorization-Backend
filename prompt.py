import os
from flask import Flask, request, jsonify
from databricks.vector_search.client import VectorSearchClient

# Initialize Flask app
app = Flask(__name__)

# Initialize environment variables for Databricks Vector Search
workspace_url = os.environ.get("WORKSPACE_URL")
sp_client_id = os.environ.get("SP_CLIENT_ID")
sp_client_secret = os.environ.get("SP_CLIENT_SECRET")

# Initialize VectorSearchClient
vsc = VectorSearchClient(
    workspace_url=workspace_url,
    service_principal_client_id=sp_client_id,
    service_principal_client_secret=sp_client_secret
)

# Function to process and format the vector search results
def process_results(results, query_text):
    if isinstance(results, dict):
        data_array = results.get('result', {}).get('data_array', [])
        if isinstance(data_array, list):
            result_texts = []
            for idx, entry in enumerate(data_array):
                if isinstance(entry, list) and len(entry) == 2:
                    content, score = entry
                    result_text = (f"Result {idx + 1}:\n"
                                   f"Content: {content}\n"
                                   f"Cosine Similarity Score: {score:.4f}\n"
                                   f"{'-' * 80}")
                    result_texts.append(result_text)
            return "\n".join(result_texts) if result_texts else "No valid results found."
        else:
            return "Unexpected format for 'data_array'."
    else:
        return "Unexpected format of results."

# API endpoint to process the query and return vector search results
@app.route('/search', methods=['POST'])
def search_query():
    try:
        # Get JSON data from the POST request
        data = request.json
        query_text = data.get("query")

        if not query_text:
            return jsonify({"error": "No query provided"}), 400

        # Perform similarity search
        index = vsc.get_index(endpoint_name="validatrix", index_name="main.default.group_six_vector_index")
        results = index.similarity_search(num_results=3, columns=["content"], query_text=query_text)

        # Process the results
        formatted_results = process_results(results, query_text)
        
        return jsonify({"query": query_text, "results": formatted_results})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)
