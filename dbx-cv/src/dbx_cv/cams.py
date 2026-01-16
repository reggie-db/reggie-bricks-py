import asyncio
from dataclasses import dataclass, field

import cv2
import numpy as np

_JPEG_START = b"\xff\xd8"
_JPEG_END = b"\xff\xd9"


@dataclass
class ProducerOptions:
    url: str
    fps: int = field(default=1)
    scale: int = field(default=640)


@dataclass
class Producer:
    task: asyncio.Task
    queue: asyncio.Queue[np.ndarray]


def producer(options: ProducerOptions) -> Producer:
    q = asyncio.Queue(maxsize=1)
    task = asyncio.create_task(_read(options, q))
    return Producer(task, q)


async def _read(options: ProducerOptions, q: asyncio.Queue):
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


async def consumer(q: asyncio.Queue):
    while True:
        frame = await q.get()
        cv2.imshow("frame", frame)
        if cv2.waitKey(1) == 27:
            break


async def main():
    rtsp_url = "rtsp://localhost:8554/cam"

    producer = asyncio.create_task(
        ffmpeg_reader(
            q,
            rtsp_url,
        )
    )
    setattr(producer, "frame_queue", q)
    consumer_task = asyncio.create_task(consumer(producer.frame_queue))

    await asyncio.gather(producer, consumer_task)


if __name__ == "__main__":
    asyncio.run(main())
