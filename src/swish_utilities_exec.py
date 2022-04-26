import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

import run
from click.testing import CliRunner

def execute(args):
    runner = CliRunner()
    result = runner.invoke(run.cli, args, catch_exceptions=False)
    return result