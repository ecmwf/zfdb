import numpy as np
import pytest

from zfdb.request import Request


def test_request_creation_datetime():
    request = Request(
        levtype="pl",
        level=[
            "50",
            "100",
            "150",
            "200",
            "250",
            "300",
            "400",
            "500",
            "600",
            "700",
            "850",
            "925",
            "1000",
        ],
        steps=list(range(0, 48)),
        date_times=[np.datetime64("2025-01-01T00:00:00")],
        params=["133", "130", "131", "132", "135", "129"],
    )

    assert request.date is None
    assert request.time is None

def test_request_creation_date_and_time():
    request = Request( levtype="pl",
        level=[
            "50",
            "100",
            "150",
            "200",
            "250",
            "300",
            "400",
            "500",
            "600",
            "700",
            "850",
            "925",
            "1000",
        ],
        steps=list(range(0, 48)),
        date=["20250101", "20250102"],
        time=["0000", "1200"],
        params=["133", "130", "131", "132", "135", "129"],
    )

    assert request.date_times == [
        np.datetime64("2025-01-01T00:00:00"),
        np.datetime64("2025-01-01T12:00:00"),
        np.datetime64("2025-01-02T00:00:00"),
        np.datetime64("2025-01-02T12:00:00"),
    ]

def test_request_creation_date_and_time_and_datetime():

    with pytest.raises(RuntimeError) as re:
        Request(
            levtype="pl",
            level=[
                "50",
                "100",
                "150",
                "200",
                "250",
                "300",
                "400",
                "500",
                "600",
                "700",
                "850",
                "925",
                "1000",
            ],
            steps=list(range(0, 48)),
            date=["20250101", "20250102"],
            time=["0000", "1200"],
            date_times=[np.datetime64("2025-01-01T00:00:00")],
            params=["133", "130", "131", "132", "135", "129"],
        )

        assert "ambiguous" in str(re) 

