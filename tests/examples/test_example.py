import zarr
from zfdb.business.FDBStore import FDBMapping


class TestExample:
    def test_tree(self):
        store = FDBMapping()
        root = zarr.group(store=store, chunk_store=None)

        ai_date_param_group = root[{"class": "ai"}][{"date": "20240601"}][
            {"param": "129"}
        ]
        subselection_group = ai_date_param_group[{"time": "0000", "levelist": "1000"}]
        tmp = subselection_group[
            {
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "levtype": "pl",
                "type": "fc",
                "step": "0",
            }
        ]
        # tmp = subselection_group[ {"domain": "g", "expver": "0001", "stream": "oper", "levtype": "pl", "type": "fc", "step": "0"} ]
        # tmp = subselection_group[ {"domain": "g", "expver": "0001", "stream": "enfo", "levtype": "pl", "type": "pf", "number": list(range(1, 51)), "step": list(range(0, 20))} ]
        # shape = (20, 50, 54200)

        print(subselection_group)
        print(subselection_group.tree())
        print(subselection_group.info)

        assert len(root) == 1584
        assert len(ai_date_param_group) == 66
        assert len(subselection_group) == 11
        assert len(tmp) == 1


if __name__ == "__main__":
    TestExample().test_tree()
