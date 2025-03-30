import asyncio
import os

import aiohttp
from tenacity import retry, stop_after_attempt, wait_incrementing
import tqdm

EMBEDDING_CACHE_PATH = os.getenv("EMBEDDING_CACHE_PATH", "cache/embeddings")
MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = os.getenv("MILVUS_PORT", 19530)
MILVUS_COLLECTION = os.getenv("MILVUS_COLLECTION", "embeddings")

LLM_CHAT_COMPLETIONS_URL = os.getenv("LLM_CHAT_COMPLETIONS_URL", "https://open.bigmodel.cn/api/paas/v4/chat/completions")
LLM_CHAT_COMPLETIONS_KEY = os.getenv("LLM_CHAT_COMPLETIONS_KEY", "xxxxxxxxxx")
LLM_CHAT_COMPLETIONS_MODEL = os.getenv("LLM_CHAT_COMPLETIONS_MODEL", "GLM-4-Flash")

class LLMDao:
    def __init__(self):
        self.url = LLM_CHAT_COMPLETIONS_URL
        self.key = LLM_CHAT_COMPLETIONS_KEY
        self.model = LLM_CHAT_COMPLETIONS_MODEL

    @retry(stop=stop_after_attempt(6), wait=wait_incrementing(start=1, increment=1))
    async def _fetch(self, session, prompt: str, semaphore: asyncio.Semaphore, pbar: tqdm.tqdm) -> str:
        async with semaphore:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.key}"
            }
            request_body = {
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0,
                "stream": False,
                "do_sample": False,
            }
            async with session.post(self.url, headers=headers, json=request_body) as response:
                response.raise_for_status()
                response_data = await response.json()
                choices = response_data.get("choices", [])
                if not choices:
                    return "Error: Missing or empty 'choices' in response"
                pbar.update(1)
                return choices[0].get("message", {}).get("content", "")

    async def _bulk_fetch(self, prompts: list[str], max_concurrent_requests) -> list[str]:
        semaphore = asyncio.Semaphore(max_concurrent_requests)
        async with aiohttp.ClientSession() as session:
            with tqdm.tqdm(total=len(prompts), desc="Requesting to LLM") as pbar:
                tasks = [self._fetch(session, prompt, semaphore, pbar) for prompt in prompts]
            return await asyncio.gather(*tasks)

    def _bulk_get_response(self, prompts: list[str], max_concurrent_requests: int) -> list[str]:
        return asyncio.run(self._bulk_fetch(prompts, max_concurrent_requests))

    def bulk_get_response_without_think(self, prompts: list[str], max_concurrent_requests: int = 20) -> list[str]:
        responses = self._bulk_get_response(prompts, max_concurrent_requests)
        return [response.split("</think>")[-1].strip() for response in responses]

    def request(self, prompt: list[str]) -> [str]:
        return self.bulk_get_response_without_think(prompt)
