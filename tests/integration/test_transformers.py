import zarr
from zfdb.business.FDBMapping import FDBMapping
from zfdb.business.transformers.FloatingPointTransformers import (
    FloatingPoint16Transformer,
)


class TestTransformer:
    def test_transformer(self):
        store = FDBMapping()
        transformer = FloatingPoint16Transformer(inner_store=store)
        root = zarr.group(store=transformer, chunk_store=None)

        ai_group = root[{"class": "ai"}]
        ai_date_group = ai_group[{"date": "20240601"}]

        ai_date_param_group = ai_date_group[{"param": "129"}]
        subselection_group = ai_date_param_group[{"time": "0000", "levelist": "1000"}]
        data = subselection_group[
            {
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "levtype": "pl",
                "type": "fc",
                "step": list(range(0, 24, 12)),
            }
        ]

        print(data.shape)
        print(data[0, 0, 0, 0])

        print(data)
        print(subselection_group.tree())
        print(subselection_group.info)

        print(ai_date_param_group)
        print(ai_date_param_group.tree())
        print(ai_date_param_group.info)

        assert len(root) == 1584
        assert len(ai_date_param_group) == 66
        assert len(subselection_group) == 11
        assert len(data) == 1


if __name__ == "__main__":
    test_class = TestTransformer()
    test_class.test_transformer()
