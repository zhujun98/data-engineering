import asyncio
from unittest.mock import patch

import pytest

from ..models.producer import AvroProducer
from ..simulation import StreamSimulation


@patch.object(AvroProducer, "produce")
@pytest.mark.parametrize('num_trains', [2, 10])
def test_cta_line(mocked_producer, num_trains):
    sim = StreamSimulation(num_trains=num_trains)
    lines = sim._cta_lines

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(
        *[li._initialize_trains() for li in lines]))

    assert len(lines[0]._stations) == 32
    assert len(lines[1]._stations) == 34
    assert len(lines[2]._stations) == 28

    # after initialization
    for i, line in enumerate(lines):
        assert line._stations[0]._a_train is None
        assert line._stations[0]._b_train.train_id == f"{line._color.capitalize()}L000"

        if num_trains == 2:
            assert line._stations[-1]._a_train.train_id == f"{line._color.capitalize()}L001"
            assert line._stations[-1]._b_train is None
        elif num_trains == 10 and i == 2:
            assert line._stations[-3]._a_train is None
            assert line._stations[-3]._b_train.train_id == f"{line._color.capitalize()}L005"

    loop.run_until_complete(asyncio.gather(*[li.run(cycles=3) for li in lines]))

    # after advancing
    for i, line in enumerate(lines):
        assert line._stations[0]._a_train is None
        assert line._stations[0]._b_train is None
        assert line._stations[3]._a_train is None
        assert line._stations[3]._b_train.train_id == f"{line._color.capitalize()}L000"

        if num_trains == 2:
            assert line._stations[-1]._a_train is None
            assert line._stations[-1]._b_train is None
            assert line._stations[-4]._a_train.train_id == f"{line._color.capitalize()}L001"
            assert line._stations[-4]._b_train is None
        elif num_trains == 10 and i == 2:
            assert line._stations[-3]._a_train is None
            assert line._stations[-3]._b_train is None
            assert line._stations[-2]._a_train.train_id == f"{line._color.capitalize()}L005"
            assert line._stations[-2]._b_train is None
