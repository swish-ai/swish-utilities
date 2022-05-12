import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

from run import run_with_args
from click.testing import CliRunner

def execute(args):
    # runner = CliRunner()
    result = run_with_args(**{})
    # result = runner.invoke(run.cli, args, catch_exceptions=False)
    return result