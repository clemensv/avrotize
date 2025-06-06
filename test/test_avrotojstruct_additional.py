import json, tempfile, os, pytest
from avrotize.avrotojstruct import AvroToJsonStructure
from jsoncomparison import Compare, NO_DIFF

@pytest.mark.parametrize("mode,expected", [
    ("pack", "packmode-ref.struct.json")
])
def test_packmode(tmp_path, mode, expected):
    avsc = tmp_path / "packmode.avsc"
    ref = os.path.join(os.getcwd(), "test/avsc", expected)
    out = tmp_path / "out.struct.json"
    # copy avsc fixture
    from shutil import copyfile
    copyfile(os.path.join(os.getcwd(), "test/avsc/packmode.avsc"), avsc)
    AvroToJsonStructure().convert(avsc.read_text(), naming_mode=mode)
    # Actually using CLI wrapper to write file
    from avrotize.avrotojstruct import convert_avro_to_json_structure
    convert_avro_to_json_structure(str(avsc), str(out), naming_mode=mode)
    actual = json.load(open(out, encoding="utf-8"))
    expected_json = json.load(open(ref, encoding="utf-8"))
    diff = Compare().check(actual, expected_json)
    assert diff == NO_DIFF
