from __future__ import annotations

import asyncio

from huggingface_hub.utils import EntryNotFoundError

from scheduler import result_persistence


class FakeUploadApi:
    def __init__(self):
        self.created = []
        self.uploaded = []

    def create_repo(self, **kwargs):
        self.created.append(kwargs)

    def upload_folder(self, **kwargs):
        self.uploaded.append(kwargs)


def test_persist_result_tree_uses_hf_dataset(tmp_path, monkeypatch):
    root = tmp_path / "results"
    uri = "by_user/u1/task_1"
    task_dir = root / uri
    task_dir.mkdir(parents=True)
    for name in ("equity_curve", "trades", "positions_daily"):
        (task_dir / f"{name}.parquet").write_bytes(b"PAR1")

    fake = FakeUploadApi()
    monkeypatch.setenv("HF_TOKEN", "test")
    monkeypatch.setattr(result_persistence, "HfApi", lambda token=None: fake)

    asyncio.run(result_persistence.persist_result_tree(uri, str(root)))

    assert fake.created
    assert fake.uploaded
    assert fake.uploaded[0]["repo_type"] == "dataset"
    assert fake.uploaded[0]["path_in_repo"] == uri


def test_ensure_result_part_local_restores_missing_file(tmp_path, monkeypatch):
    root = tmp_path / "results"
    uri = "by_user/u1/task_2"
    monkeypatch.setenv("HF_TOKEN", "test")

    def fake_download(**kwargs):
        target = root / uri / "equity_curve.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"PAR1")
        return str(target)

    monkeypatch.setattr(result_persistence, "hf_hub_download", fake_download)

    assert asyncio.run(
        result_persistence.ensure_result_part_local(uri, "equity_curve", str(root))
    ) is True


def test_ensure_result_part_local_raises_on_hf_failure(tmp_path, monkeypatch):
    root = tmp_path / "results"
    uri = "by_user/u1/task_3"
    monkeypatch.setenv("HF_TOKEN", "test")

    def failed_download(**kwargs):
        raise RuntimeError("hf unavailable")

    monkeypatch.setattr(result_persistence, "hf_hub_download", failed_download)

    try:
        asyncio.run(
            result_persistence.ensure_result_part_local(uri, "equity_curve", str(root))
        )
    except RuntimeError as exc:
        assert "hf unavailable" in str(exc)
    else:
        raise AssertionError("expected durable restore failure to propagate")
