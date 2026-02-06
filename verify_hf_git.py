import requests
import json
import base64

BASE_URL = "http://localhost:8000/api"
GIT_URL = "http://localhost:8000"
TOKEN = "dummy-token" # In a real test, we'd get this from login

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

def test_availability():
    print("Testing name availability...")
    resp = requests.get(f"{BASE_URL}/repos/create?name=test-repo&type=model", headers=headers)
    print(f"Status: {resp.status_code}, Body: {resp.json()}")

def test_creation():
    print("\nTesting model creation...")
    payload = {
        "name": "test-hf-model",
        "private": False
    }
    resp = requests.post(f"{BASE_URL}/models", json=payload, headers=headers)
    print(f"Status: {resp.status_code}, Body: {resp.json()}")

def test_git_refs():
    print("\nTesting Git info/refs...")
    # This should advertise HEAD and refs/heads/main (at least)
    resp = requests.get(f"{GIT_URL}/offsec/test-hf-model.git/info/refs?service=git-upload-pack")
    print(f"Status: {resp.status_code}")
    print("Content (first 100 bytes):")
    print(resp.content[:100])

def test_metadata_extraction():
    print("\nTesting README metadata extraction...")
    # We commit a README with YAML frontmatter
    readme_content = """---
license: mit
pipeline_tag: text-classification
tags:
- nlp
- pytorch
---
# Test Model
"""
    readme_b64 = base64.b64encode(readme_content.encode()).decode()
    
    payload = [
        {"key": "header", "value": {"summary": "Update README"}},
        {"key": "file", "value": {"path": "README.md", "content": readme_b64, "encoding": "base64"}}
    ]
    
    ndjson = "\n".join(json.dumps(line) for line in payload)
    
    resp = requests.post(
        f"{BASE_URL}/models/offsec/test-hf-model/commit/main", 
        data=ndjson, 
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/x-ndjson"}
    )
    print(f"Commit Status: {resp.status_code}")
    
    # Check repo metadata
    resp = requests.get(f"{BASE_URL}/models/offsec/test-hf-model", headers=headers)
    repo_info = resp.json()
    print(f"Metadata - License: {repo_info.get('license')}")
    print(f"Metadata - Pipeline: {repo_info.get('pipeline_tag')}")
    print(f"Metadata - Tags: {repo_info.get('tags')}")

if __name__ == "__main__":
    # Note: This script assumes the server is running and "offsec" user exists with a valid token.
    # Since I cannot run the actual server and interact with it via requests here easily,
    # I'll rely on unit tests and code inspection.
    print("Verification script created. Run against a local server instance.")
