# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import yaml

from zfdb.mapping import extract_mars_requests_from_recipe


def test_recipe_parsing(data_path):
    recipe_file = (
        data_path
        / "recipes"
        / "aifs-ea-an-oper-0001-mars-o96-2024-2024-6h-v1-january.yaml"
    )
    recipe = yaml.safe_load(recipe_file.read_text())
    start_date, end_date, frequency, requests = extract_mars_requests_from_recipe(
        recipe
    )
    assert start_date == "2024-01-01T00:00:00"
    assert end_date == "2024-01-31T18:00:00"
    assert frequency[0] == 6 and frequency[1] == "h"
    assert len(requests) == 2
    print(requests)
