import shelve

import numpy as np
from sentence_transformers import SentenceTransformer
import tqdm

from api.BaseEncode import UDFBaseEncode
from default.DefaultUtilities import EMBEDDING_CACHE_PATH


class UDFDefaultEncode(UDFBaseEncode):
    def __init__(self, cache_path=EMBEDDING_CACHE_PATH):
        print("Initializing Sentence Transformer")
        self.encoder = SentenceTransformer("paraphrase-mpnet-base-v2")
        self.batch_size = 128
        self.cache_path = cache_path

    def encode(self, descriptions: list[str]) -> list[np.ndarray[np.float32]]:
        print("Calculating embeddings of", len(descriptions), "descriptions")

        # 打开 shelve 缓存
        with shelve.open(self.cache_path) as cache:
            # 找出未缓存的描述
            uncached_descriptions = [desc for desc in descriptions if desc not in cache]
            print("Uncached descriptions:", len(uncached_descriptions))

            # 计算未缓存的 embeddings
            if uncached_descriptions:
                description_batches = [
                    uncached_descriptions[i:i + self.batch_size]
                    for i in range(0, len(uncached_descriptions), self.batch_size)
                ]

                with tqdm.tqdm(total=len(uncached_descriptions), desc="Calculating embeddings") as pbar:
                    for description_batch in description_batches:
                        embedding_batch = self.encoder.encode(description_batch)
                        for desc, embedding in zip(description_batch, embedding_batch):
                            cache[desc] = embedding  # 存入缓存
                        cache.sync()  # 立即写入
                        pbar.update(len(description_batch))

            # 读取缓存
            embeddings = [cache[desc] for desc in descriptions]

        return embeddings


if __name__ == "__main__":
    # get temp file path
    udf = UDFDefaultEncode()
    data = [['description'], ['BINARY'], ['hello'.encode()], ['world'.encode()]]
    result = udf.transform(data, [], {})
    print(result)
