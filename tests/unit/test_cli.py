from click.testing import CliRunner

import etcdb
from etcdb.cli import main


def test_version():
    runner = CliRunner()
    result = runner.invoke(main, ['--version'])
    assert result.exit_code == 0
    assert result.output == etcdb.__version__ + '\n'
