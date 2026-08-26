"""HF token 最小验证：身份 + 数据集可访问性 + 关键文件存在性（不做大文件下载）。"""
import os
import sys

TOKEN = os.getenv("HF_TOKEN")
if not TOKEN:
    print("ERROR: HF_TOKEN not set")
    sys.exit(1)

from huggingface_hub import HfApi

api = HfApi(token=TOKEN)

# 1) 身份
try:
    who = api.whoami()
    name = who.get("name") or who.get("fullname") or "?"
    print(f"[1] whoami OK: {name} (authType={who.get('authType', '?')})")
except Exception as e:
    print(f"[1] whoami FAILED: {e}")
    sys.exit(2)

# 2) 数据集可访问性
REPO = "scanli/stocka-data"
try:
    info = api.dataset_info(REPO)
    print(f"[2] dataset OK: {REPO} (private={info.private}, sha={str(info.sha)[:8]})")
except Exception as e:
    print(f"[2] dataset FAILED: {e}")
    sys.exit(3)

# 3) 关键文件存在性
try:
    files = api.list_repo_files(repo_id=REPO, repo_type="dataset")
    kline = sorted(f for f in files if f.startswith("stock_kline_"))
    print(f"[3] files OK: total={len(files)}, stock_kline count={len(kline)}")
    print(f"    years: {[f.split('_')[-1].replace('.parquet','') for f in kline]}")
    for need in ("stock_kline_2024.parquet", "stock_kline_2025.parquet"):
        print(f"    {'OK ' if need in files else 'MISSING '} {need}")
except Exception as e:
    print(f"[3] list_files FAILED: {e}")
    sys.exit(4)

print("\nTOKEN VALID [OK]")