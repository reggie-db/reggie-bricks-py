import asyncio
import contextvars

import uvicorn
from starlette.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request
from demo_iot.apis import APIImplementation
from demo_iot_generated.main import GeneratedRouter
from reggie_core import logs
from reggie_tools import configs

LOG = logs.logger(__file__)


def main():
    app = FastAPI()
    current_request_ctx = contextvars.ContextVar("current_request")
    api = APIImplementation(configs.get(), lambda: current_request_ctx.get(None))
    app.include_router(GeneratedRouter(api).router)

    @app.middleware("http")
    async def store_request(request: Request, call_next):
        token = current_request_ctx.set(request)
        try:
            return await call_next(request)
        finally:
            current_request_ctx.reset(token)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # or specific domains: ["https://example.com"]
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    if "loop_factory" not in asyncio.run.__code__.co_varnames:
        uvicorn.server.asyncio_run = lambda coro, **_: asyncio.run(coro)
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
