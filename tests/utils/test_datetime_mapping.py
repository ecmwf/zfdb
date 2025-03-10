from datetime import datetime

from zfdb.utils.date_mapping import mars_date_to_date, mars_time_to_time


def test_mapping_date():
    mapped_date = mars_date_to_date("20210101")

    assert mapped_date.year == 2021
    assert mapped_date.month == 1
    assert mapped_date.day == 1

def test_mapping_date_dashed():
    mapped_date = mars_date_to_date("2021-01-01")

    assert mapped_date.year == 2021
    assert mapped_date.month == 1
    assert mapped_date.day == 1

def test_mapping_time():
    mapped_time = mars_time_to_time("1130")

    assert mapped_time.hour == 11
    assert mapped_time.minute == 30

def test_mapping_time_colon():
    mapped_time = mars_time_to_time("12:59")

    assert mapped_time.hour == 12
    assert mapped_time.minute == 59
