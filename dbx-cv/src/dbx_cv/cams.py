import asyncio
from dataclasses import dataclass, field

import cv2
import numpy as np

"""Async RTSP frame producer and consumer helpers for local CV workflows."""

_JPEG_START = b"\xff\xd8"
_JPEG_END = b"\xff\xd9"


@dataclass
class ProducerOptions:
    """Configuration for ffmpeg-based RTSP frame extraction."""

    url: str
    fps: int = field(default=1)
    scale: int = field(default=640)


@dataclass
class Producer:
    """Container for the producer task and most-recent-frame queue."""

    task: asyncio.Task
    queue: asyncio.Queue[np.ndarray]


def producer(options: ProducerOptions) -> Producer:
    """Start an async frame producer for the configured RTSP stream."""
    q = asyncio.Queue(maxsize=1)
    task = asyncio.create_task(_read(options, q))
    return Producer(task, q)


async def _read(options: ProducerOptions, q: asyncio.Queue) -> None:
    cmd = [
        "ffmpeg",
        "-rtsp_transport",
        "tcp",
        "-i",
        options.url,
        "-vf",
        f"fps={options.fps},scale=-1:{options.scale}",
        "-q:v",
        "2",
        "-f",
        "mjpeg",
        "-",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
    )

    buffer = b""

    while True:
        chunk = await proc.stdout.read(4096)
        if not chunk:
            break

        buffer += chunk

        start = buffer.find(_JPEG_START)
        end = buffer.find(_JPEG_END)

        if start != -1 and end != -1:
            jpeg = buffer[start : end + 2]
            buffer = buffer[end + 2 :]

            frame = cv2.imdecode(np.frombuffer(jpeg, np.uint8), cv2.IMREAD_COLOR)
            if frame is not None:
                while True:
                    try:
                        q.put_nowait(frame)
                    except asyncio.QueueFull:
                        try:
                            q.get_nowait()
                        except asyncio.QueueEmpty:
                            pass


async def consumer(q: asyncio.Queue) -> None:
    """Display frames from the queue until ESC is pressed."""
    while True:
        frame = await q.get()
        cv2.imshow("frame", frame)
        if cv2.waitKey(1) == 27:
            break


async def main() -> None:
    """Run producer and consumer tasks for local RTSP testing."""
    rtsp_url = "rtsp://localhost:8554/cam"
    frame_producer = producer(ProducerOptions(url=rtsp_url))
    consumer_task = asyncio.create_task(consumer(frame_producer.queue))
    await asyncio.gather(frame_producer.task, consumer_task)


if __name__ == "__main__":
    asyncio.run(main())
