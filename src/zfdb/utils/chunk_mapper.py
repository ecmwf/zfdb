class ChunkMapper:
    def __init__(self, chunk_dimensions: tuple[int, ...]):
        self.chunk_dimensions = chunk_dimensions

    def map(self, chunk_index: tuple[int, ...]) -> dict[str, slice]:
        pass


class StepChunkMapper(ChunkMapper):
    def map(self, chunk_index: tuple[int, ...]) -> dict[str, slice]:
        if len(chunk_index) != len(self.chunk_dimensions):
            raise RuntimeError(
                f"Mismatch in dimensions, chunking set up for {len(self.chunk_dimensions)}, but asked for chunk {chunk_index}"
            )

        result = {}

        result["step"] = slice(0, 1)
        result["date"] = slice(0, None)
        result["time"] = slice(0, None)
        result["param"] = slice(0, None)

        return result
