from json import JSONEncoder


class ZarrMetadataBuilder:
    """
    Create json files for zarr metadata similar to the one below.
                { 
                    "zarr_format": 2,
                    "dtype": "<f4",
                    "shape": [100, 100],
                    "fill_value": "NaN",
                    "chunks": "[1, 1]",
                    "compressor": { 
                        "blocksize": 0, 
                        "clevel": 5, 
                        "cname": "lz4", 
                        "id": "blosc", 
                        "shuffle": 1 
                    }, 
                    "order": "C",
                    "filters": null
                }
    """

    def __init__(self):
        self._zarr_format = 2
        self._dtype = None
        self._shape = None
        self._fill_value =None
        self._chunks = None
        self._compressor =None
        self._order = None
        self._filters = None

    @staticmethod
    def default(mars_request):
        """
        This is for now returning hard-coded values for the field sizes
        as well as setting up things to work with the gribjump axis object
        """

        time_dim = len(mars_request['date'])
        param_dim = len(mars_request['param'])

        metadata = ZarrMetadataBuilder()
        metadata.zarr_format(2)
        metadata.dtype("float64")
        metadata.shape([time_dim, 1, param_dim, 542080])
        metadata.fill_value(0)
        metadata.chunks([time_dim, 1, param_dim, 542080])
        metadata.compressor(None)
        metadata.order("C")
        metadata.filters(None)

        return metadata.build()
    
    def zarr_format(self, zarr_Version):
        self._zarr_format = zarr_Version
        return self

    def dtype(self, dtype):
        self._dtype = dtype
        return self

    def shape(self, shape):
        self._shape = shape
        return self

    def fill_value(self, fill_value):
        self._fill_value = fill_value
        return self

    def chunks(self, chunks):
        self._chunks = chunks
        return self

    def compressor(self, compressor):
        self._compressor = compressor
        return self

    def order(self, order):
        self._order = order
        return self

    def filters(self, filters):
        self._filters = filters
        return self

    def build(self):
        self_dict = {key.lstrip("_"):value for key, value in self.__dict__.items() if not key.startswith('__') and not callable(key)}
        return JSONEncoder().encode(self_dict)
