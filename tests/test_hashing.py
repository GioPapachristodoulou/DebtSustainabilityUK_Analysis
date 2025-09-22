from pipeline.utils.hashing import file_sha256
def test_hashing_roundtrip(tmp_path):
    p = tmp_path/"x.txt"; p.write_text("abc")
    assert len(file_sha256(p)) == 64
