
import os
import sys
from fastapi.testclient import TestClient
from unittest.mock import patch

# Adjust path to include src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from kohakuhub.main import app
from kohakuhub.config import cfg

def test_dataset_viewer_api():
    client = TestClient(app)
    
    print("Testing Dataset Viewer API against Hugging Face Hub (mocking local endpoint)...")
    
    # We patch the base_url to point to HF so 'datasets' library fetches from there
    # This verifies our API logic correctly wraps 'datasets' library.
    
    # Using a small dataset: 'lhoestq/demo1' (if it exists) or 'allocine'
    # 'lhoestq/test'
    # Let's use 'rotten_tomatoes' which is small. Repo ID: 'rotten_tomatoes'.
    # But our API enforces namespace/repo structure in URL: /api/datasets/{namespace}/{repo}/...
    # So we need a namespaced dataset.
    # 'imdb' is 'stanfordnlp/imdb' ? No, usually just 'imdb' or 'stanfordnlp/imdb'.
    # Let's try 'huggingface/documentation-images' (dataset?)
    # Best is a user dataset. 
    # Let's try 'severo/test-dataset' (often used for testing).
    
    namespace = "severo"
    repo = "test-dataset" # Hopefully this exists or something similar.
    # Actually, let's use 'glue' and pretend namespace is 'glue' implies repo 'glue'? No.
    # We can use 'fancy-dataset/dataset' style.
    # 'polinaeterna/test_dataset'
    
    # Let's just mock `_get_hf_endpoint` to return https://huggingface.co
    # And we also need to allow the router to authorize us.
    # The routers.py uses `system_token` which is `cfg.admin.secret_token`.
    # Code uses: `token = system_token`.
    # If we point to HF, we don't want to pass our local admin token to HF!
    # HF will reject it or ignore it.
    # We should pass None or a valid HF token if needed.
    # For public datasets, token=None is fine.
    
    # We will patch `viewer._get_hf_endpoint` and `viewer.get_dataset_info`'s token usage?
    # No, let's just patch `viewer._get_hf_endpoint` to return "https://huggingface.co".
    # And we must ensure `routers.py` connects.
    
    with patch("kohakuhub.api.datasets.viewer._get_hf_endpoint", return_value="https://huggingface.co"):
        # We also need to patch the token passed to `get_dataset_info` in routers.py to be None
        # OR just handle the fact that passing a garbage token to HF for public repo usually works (it just warns) or fails.
        # It might fail with 401.
        
        # Let's patch `kohakuhub.api.datasets.routers.viewer.get_dataset_info`? 
        # No, we want to test `viewer.get_dataset_info`.
        
        # Let's assume we can fetch 'rotten_tomatoes' but we need to map namespace/repo.
        # URL: /api/datasets/rotten/tomatoes/viewer/info -> repo_id "rotten/tomatoes".
        # This doesn't exist.
        
        # We need a real namespaced dataset.
        # 'c4' -> 'allenai/c4'
        namespace = "allenai"
        repo = "c4"
        
        # Note: loading C4 is heavy.
        # Let's use 'lhoestq/demo1'
        namespace = "lhoestq"
        repo = "demo1"
        
        print(f"Fetching info for {namespace}/{repo}...")
        response = client.get(f"/api/datasets/{namespace}/{repo}/viewer/info")
        
        if response.status_code != 200:
            print(f"Failed: {response.status_code}")
            print(response.json())
        else:
            print("Success (Info)!")
            data = response.json()
            print("Configs:", data.get("configs"))
            if "default" in data.get("configs", []):
                print("Found default config.")
                
        # Now fetch rows
        print(f"Fetching rows for {namespace}/{repo}...")
        response = client.get(f"/api/datasets/{namespace}/{repo}/viewer/rows", params={
            "config": "default",
            "split": "train",
            "limit": 5
        })
        
        if response.status_code != 200:
            print(f"Failed: {response.status_code}")
            print(response.json())
        else:
            print("Success (Rows)!")
            data = response.json()
            rows = data.get("rows", [])
            print(f"Got {len(rows)} rows.")
            if len(rows) > 0:
                print("Sample row:", rows[0])

if __name__ == "__main__":
    test_dataset_viewer_api()
