import os
from huggingface_hub import list_repo_files

repo_id = "scanli/stocka-data"
hf_token = os.getenv("HF_TOKEN") # Assuming HF_TOKEN is available in the environment

try:
    files = list_repo_files(repo_id=repo_id, repo_type="dataset", token=hf_token)
    with open("hf_repo_files.txt", "w") as f:
        for file in files:
            f.write(file + "\n")
except Exception as e:
    with open("hf_repo_files.txt", "w") as f:
        f.write(f"Error: {e}\n")
