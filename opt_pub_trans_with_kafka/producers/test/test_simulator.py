import pathlib

import pytest
import pandas as pd

from ..simulator import TimeSimulator


def test_line():
    sim = TimeSimulator()

    line = sim._train_lines[0]
    print(line)
