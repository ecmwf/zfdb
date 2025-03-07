from datetime import date, time
from datetime import datetime as dt


def mars_date_to_date(str) -> date:
    try:
        return dt.strptime(str, "%Y%m%d").date()
    except ValueError:
        pass

    try:
        return dt.strptime(str, "%Y-%m-%d").date()
    except ValueError:
        pass

    raise RuntimeError(
        "Wrong format for mars date. Either YYYYMMDD or YYYY-MM-DD are supported."
    )


def mars_time_to_time(str) -> time:
    try:
        return dt.strptime(str, "%H%M").time()
    except ValueError:
        pass

    try:
        return dt.strptime(str, "%H:%M").time()
    except ValueError:
        pass

    raise RuntimeError(
        "Wrong format for mars date. Either HHMM or HH:MM are supported."
    )
