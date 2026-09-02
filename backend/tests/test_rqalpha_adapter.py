"""Tests for RQAlpha adapter."""
from core.rqalpha_adapter import _to_rqalpha_code, _from_rqalpha_code


def test_code_roundtrip():
    codes = ["sh.600000", "sz.000001", "bj.830799"]
    for c in codes:
        assert _from_rqalpha_code(_to_rqalpha_code(c)) == c


def test_to_rqalpha_format():
    assert _to_rqalpha_code("sh.600000") == "600000.XSHG"
    assert _to_rqalpha_code("sz.000001") == "000001.XSHE"
    assert _to_rqalpha_code("bj.830799") == "830799.XBJE"


def test_from_rqalpha_format():
    assert _from_rqalpha_code("600000.XSHG") == "sh.600000"
    assert _from_rqalpha_code("000001.XSHE") == "sz.000001"
