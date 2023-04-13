import pytest

import os

from delphi_utils import read_params
from delphi_utils.utils import version_check


class TestReadParams:
    def test_return_params(self):
        params = read_params()
        assert params["test"] == "yes"

    def test_copy_template(self):
        os.remove("params.json")
        params = read_params()
        assert params["test"] == "yes"

class TestVersionCheck:
    def test_version_expected(self):
        with open("version.cfg", "w") as ver_file:
            ver_file.write("current_version = 0.3.35")
        current_version = version_check()
        os.remove("version.cfg")
        assert current_version == "0.3.35"

    def test_version_unexpected(self):
        with open("version.cfg", "w") as ver_file:
            ver_file.write("currrent_verssion = 0.3.35")
        current_version = version_check()
        os.remove("version.cfg")
        assert current_version == "not found"

    def test_version_missing(self):
        current_version = version_check()
        assert current_version == "not found"


