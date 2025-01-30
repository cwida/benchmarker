import os
import sys
root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)


def speedup_factor(a: float, b: float) -> float:
    return a / b