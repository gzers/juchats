import asyncio
import json
import typing
from uuid import uuid4
import time

import httpx
import pydantic
import websockets
from loguru import logger
import redislite

from .configs import APIS, HEADERS
from .types import Mode


def get_headers(token: str):
    headers = HEADERS
    headers['jtoken'] = token
    return headers


class Juchats(object):
    _modes = None
    _redis = redislite.Redis('/tmp/juchats_redis.db')

    def __init__(self, token: str, model: str = "deepseek-ai/deepseek-r1"):
        self.token = token
        self.model = model
        self._header = get_headers(token)
        self._model_id = None
        self._dialog_id = None
        self._initialized = False

    async def __aenter__(self):
        """异步上下文管理器入口"""
        # 执行异步初始化操作（例如建立网络连接、验证 API Key 等）
        await self._async_connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        # 执行异步清理操作（例如关闭连接）
        await self._async_close()

    async def initialize(self):
        if self._initialized:
            return

        available_models = await self.get_models()
        for mode in available_models:
            if mode.name == self.model:
                self._model_id = mode.id
                break
        else:
            print(available_models)
            for mode in available_models:
                print(mode.id, mode.name)
            raise ValueError(f"Model {self.model} not found")

        self._initialized = True

    async def _ensure_initialized(self):
        if not self._initialized:
            await self.initialize()

    async def get_models(self) -> typing.List[Mode]:
        # Try to get models from Redis cache
        cached_models = self._redis.get('models')
        if cached_models:
            logger.info("get models from cache")
            cached_data = json.loads(cached_models)
            if time.time() < cached_data['expiry']:
                return [Mode(**x) for x in cached_data['modes']]

        # If not in cache or expired, fetch from API
        async with httpx.AsyncClient() as client:
            response = await client.get(APIS.MODES, headers=self._header)
            data = response.json()['data']
            modes = []
            for item in data:
                modes.extend([Mode(**x) for x in item['modes']])

            # Cache the result in Redis with 1-hour expiry
            cache_data = {
                'modes': [mode.dict() for mode in modes],
                'expiry': time.time() + 3600  # 1 hour from now
            }
            self._redis.set('models', json.dumps(cache_data))

            return modes

    async def get_dialog_id(self) -> int:
        async with httpx.AsyncClient() as client:
            response = await client.post(APIS.DIALOGS,
                                         headers=self._header,
                                         json={})
            response.raise_for_status()
            data = response.json()['data']
            for x in data:
                if x['modeId'] == self._model_id:
                    return x['id']

            logger.info('create new dialog for model {}'.format(self.model))
            _type = await self.get_type()
            response = await client.post(APIS.CREATE_DIALOG,
                                         headers=self._header,
                                         json={
                                             'dialogType': 1,
                                             'name': self.model,
                                             'type': _type,
                                             'ttsLanguageTypeId': 0,
                                             'ttsType': 0,
                                             'modeId': self._model_id,
                                             'contextId': '',
                                         })
            response.raise_for_status()
            return int(response.json()['data'])

    async def get_model_id(self) -> int:
        return self._model_id

    async def get_type(self) -> int:
        return 10

    async def chat(
        self,
        query: str,
        show_stream: bool = False,
    ):
        await self._ensure_initialized()
        dialog_id = await self.get_dialog_id()
        model_id = await self.get_model_id()
        _type = await self.get_type()
        async with websockets.connect(APIS.WSS.format(self.token),
                                      extra_headers=self._header) as ws:
            message = {
                "contextId": '',
                "dialogId": dialog_id,
                "event": 1,
                "fileUuid": "",
                "languageTypeId": 0,
                "modeId": model_id,
                "prompt": query,
                "requestId": str(uuid4()),
                "type": _type,
            }

            await ws.send(json.dumps(message))

            try:
                text = ''
                while True:
                    response = await ws.recv()
                    if '[DONE]' in response:
                        return text
                    js = json.loads(response)
                    content = js.get('data', {}).get('content')
                    if content:
                        text += content
                        if show_stream:
                            print(content, end="", flush=True)
                    if int(js.get('code', 200)) != 200:
                        logger.info(js)
                        return text
            except websockets.ConnectionClosed:
                logger.error("Connection closed by the server.")
            except Exception as e:
                logger.error(f"Error occurred: {e}")

    async def chat2(self, query: str):
        await self._ensure_initialized()
        dialog_id = await self.get_dialog_id()
        model_id = await self.get_model_id()
        _type = await self.get_type()

        message = {
            "contextId": '',
            "dialogId": dialog_id,
            "event": 1,
            "fileUuid": "",
            "languageTypeId": 0,
            "modeId": model_id,
            "prompt": query,
            "requestId": str(uuid4()),
            "type": _type,
            "tools": {
                "id": "BROWSING",
                "name": "Browsing"
            }
        }

        url = APIS.SSE.format(self.token)
        headers = self._header
        headers['Content-Type'] = 'application/json'
        headers['Accept'] = 'text/event-stream'

        async with httpx.AsyncClient(headers=headers) as client:
            try:
                text = ''
                response = await client.post(url, json=message, timeout=60.0)
                logger.info(f"Response status: {response.status_code}")
                if response.status_code == 200:
                    event_data = ""
                    event_type = "message"
                    async for line in response.aiter_lines():
                        line = line.strip()
                        if not line:
                            # 空行表示事件结束
                            if event_type == "message" and event_data:
                                try:
                                    data = json.loads(event_data)
                                    content = data.get('data', {}).get('content')
                                    if content:
                                        text += content
                                    if int(data.get('code', 200)) != 200:
                                        logger.info(data)
                                        return text
                                    else:
                                        logger.warning(f"Received response without content: {data}")
                                except json.JSONDecodeError:
                                    logger.error(f"Failed to decode JSON: {event_data}")
                            elif event_type == "done":
                                logger.info("Received [DONE], closing connection.")
                                return text
                            event_data = ""
                            event_type = "message"
                        elif line.startswith('data:'):
                            if line == 'data:[DONE]':
                                event_type = "done"
                            else:
                                event_data += line[len('data:'):].strip() + "\n"
                        elif line.startswith('event:'):
                            event_type = line[len('event:'):].strip()
                else:
                    logger.error(f"Request failed with status {response.status_code}")
            except httpx.ReadTimeout as rt:
                logger.error(f"Request timed out: {rt}")
            except httpx.RequestError as re:
                logger.error(f"Request error: {re}")

    async def stream_chat(
        self,
        query: str,
    ):
        await self._ensure_initialized()
        dialog_id = await self.get_dialog_id()
        model_id = await self.get_model_id()
        _type = await self.get_type()
        async with websockets.connect(APIS.WSS.format(self.token),
                                      extra_headers=self._header) as ws:
            message = {
                "contextId": '',
                "dialogId": dialog_id,
                "event": 1,
                "fileUuid": "",
                "languageTypeId": 0,
                "modeId": model_id,
                "prompt": query,
                "requestId": str(uuid4()),
                "type": _type,
            }

            await ws.send(json.dumps(message))

            try:
                while True:
                    response = await ws.recv()
                    if '[DONE]' in response:
                        break
                    js = json.loads(response)
                    content = js.get('data', {}).get('content')
                    if content:
                        yield content
                    if int(js.get('code', 200)) != 200:
                        logger.info(js)
                        break
            except websockets.ConnectionClosed:
                logger.error("Connection closed by the server.")
            except Exception as e:
                logger.error(f"Error occurred: {e}")

    async def stream_chat2(self, query: str):
        await self._ensure_initialized()
        dialog_id = await self.get_dialog_id()
        model_id = await self.get_model_id()
        _type = await self.get_type()

        message = {
            "contextId": '',
            "dialogId": dialog_id,
            "event": 1,
            "fileUuid": "",
            "languageTypeId": 0,
            "modeId": model_id,
            "prompt": query,
            "requestId": str(uuid4()),
            "type": _type,
            "tools": {
                "id": "BROWSING",
                "name": "Browsing"
            }
        }

        url = APIS.SSE.format(self.token)
        headers = self._header
        headers['Content-Type'] = 'application/json'
        headers['Accept'] = 'text/event-stream'

        async with httpx.AsyncClient(headers=headers) as client:
            try:
                response = await client.post(url, json=message, timeout=60.0)
                logger.info(f"Response status: {response.status_code}")
                if response.status_code == 200:
                    event_data = ""
                    event_type = "message"
                    async for line in response.aiter_lines():
                        line = line.strip()
                        if not line:
                            # 空行表示事件结束
                            if event_type == "message" and event_data:
                                try:
                                    data = json.loads(event_data)
                                    content = data.get('data', {}).get('content')
                                    if content:
                                        yield content
                                    else:
                                        logger.warning(f"Received response without content: {data}")
                                except json.JSONDecodeError:
                                    logger.error(f"Failed to decode JSON: {event_data}")
                            elif event_type == "done":
                                logger.info("Received [DONE], closing connection.")
                                break
                            event_data = ""
                            event_type = "message"
                        elif line.startswith('data:'):
                            if line == 'data:[DONE]':
                                event_type = "done"
                            else:
                                event_data += line[len('data:'):].strip() + "\n"
                        elif line.startswith('event:'):
                            event_type = line[len('event:'):].strip()
                else:
                    logger.error(f"Request failed with status {response.status_code}")
            except httpx.ReadTimeout as rt:
                logger.error(f"Request timed out: {rt}")
            except httpx.RequestError as re:
                logger.error(f"Request error: {re}")

    async def clear_chats(self) -> int:
        async with httpx.AsyncClient() as client:
            await self._ensure_initialized()
            _type = await self.get_type()
            dialog_id = await self.get_dialog_id()
            logger.info(dialog_id)
            _type = await self.get_type()
            response = await client.post(APIS.CLEAR_CHATS,
                                         headers=self._header,
                                         json={
                                             "id": dialog_id,
                                         })
            response.raise_for_status()
            return response.json()

    async def _async_close(self):
        pass

    async def _async_connect(self):
        pass
