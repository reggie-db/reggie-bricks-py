import asyncio
import base64
import os
import time
from typing import AsyncIterable, Optional

import cv2
import numpy as np
from reggie_core import logs
from reggie_tools_rx import events
from reggie_tools_rx.events import OutMessage

LOG = logs.logger(__file__)
MAX_FPS = 30.0
MIN_FRAME_INTERVAL = 1.0 / MAX_FPS  # Minimum time between frames (seconds)


def extract_client_id(msg: OutMessage, client_id_key: str) -> Optional[str]:
    """Extract client_id from message headers or messageId, return None if not found."""
    client_id = next(
        (h.value for h in msg.headers or [] if h.key == client_id_key),
        None,
    )

    if not client_id:
        # Try to extract from messageId if not in headers
        if hasattr(msg, "messageId") and msg.messageId:
            parts = msg.messageId.split("_")
            if len(parts) > 0:
                client_id = parts[0]

    return client_id


async def event_reader(
    stream: AsyncIterable[OutMessage],
    frame_queue: asyncio.Queue,
    client_id_key: str = "client_id",
    stream_id_key: str = "stream_id",
):
    """Read events from stream and put valid frames into the queue.

    Discards messages without a valid client_id.
    Checks FPS capacity BEFORE parsing to avoid unnecessary work.
    """
    client_fps_tracker: dict[str, float] = {}
    try:
        async for msg in stream:
            try:
                client_id = extract_client_id(msg, client_id_key)
                if not client_id:
                    # Discard message if client_id not found
                    continue

                # Check frame rate capacity BEFORE parsing (first check)
                last_frame_time = client_fps_tracker.get(client_id, None)

                if (
                    last_frame_time is not None
                    and (time.time() - last_frame_time) < MIN_FRAME_INTERVAL
                ):
                    # Skip this frame without parsing
                    continue

                try:
                    # Get stream_id from headers (used for window title)
                    stream_id = next(
                        (h.value for h in msg.headers or [] if h.key == stream_id_key),
                        client_id,
                    )

                    # Get resolution from headers
                    width = next(
                        (int(h.value) for h in msg.headers or [] if h.key == "width"),
                        None,
                    )
                    height = next(
                        (int(h.value) for h in msg.headers or [] if h.key == "height"),
                        None,
                    )

                    # Parse base64 image (only if not over capacity)
                    b64_image = msg.value.value
                    if b64_image.startswith("data:image"):
                        b64_image = b64_image.split(",", 1)[1]
                    img_bytes = base64.b64decode(b64_image)
                    np_arr = np.frombuffer(img_bytes, np.uint8)
                    frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

                    if frame is not None:
                        # Put frame data into queue
                        await frame_queue.put(
                            {
                                "client_id": client_id,
                                "stream_id": stream_id,
                                "width": width,
                                "height": height,
                                "frame": frame,
                            }
                        )
                finally:
                    client_fps_tracker[client_id] = time.time()

            except Exception as e:
                LOG.exception(f"Error processing message: {e}")

    except Exception as e:
        LOG.exception(f"Error in event reader: {e}")
    finally:
        # Signal end of stream
        await frame_queue.put(None)


async def display_frames(
    stream: AsyncIterable[OutMessage],
    client_id_key: str = "client_id",
    stream_id_key: str = "stream_id",
    timeout: float = 3.0,
    cleanup_interval: float = 0.5,
):
    """Display frames in a separate window per client_id, with window title showing stream_id.

    Windows are tracked by client_id and timeout based on client_id activity.
    Window titles display the stream_id for identification.

    All OpenCV window operations run on the main thread to prevent freezing.
    Uses a while True loop with a queue to ensure cleanup happens even without messages.
    """

    windows: dict[str, dict] = {}  # client_id -> {"last_seen": float, "stream_id": str}
    last_cleanup = time.time()
    frame_queue: asyncio.Queue = asyncio.Queue()

    # Start async task to read events and populate queue
    reader_task = asyncio.create_task(
        event_reader(stream, frame_queue, client_id_key, stream_id_key)
    )

    try:
        while True:
            current_time = time.time()

            # Cleanup inactive windows periodically (even without messages)
            if current_time - last_cleanup >= cleanup_interval:
                inactive = [
                    cid
                    for cid, meta in windows.items()
                    if current_time - meta["last_seen"] > timeout
                ]
                for cid in inactive:
                    LOG.info(f"Closing inactive window for client {cid}")
                    # OpenCV operations must run on main thread
                    try:
                        cv2.destroyWindow(cid)
                    except Exception as e:
                        LOG.warning(f"Error destroying window {cid}: {e}")
                    del windows[cid]
                last_cleanup = current_time

            # Try to get a frame from the queue (non-blocking with timeout)
            frame_data = None
            if not reader_task.done() or not frame_queue.empty():
                try:
                    frame_data = await asyncio.wait_for(frame_queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    frame_data = None

            # Check for sentinel value or quit (end of stream)
            if (frame_data is None and reader_task.done()) or (
                cv2.waitKey(1) & 0xFF == ord("q")
            ):
                break

            if frame_data is None:
                continue

            # Process frame (FPS check already done in event_reader)
            client_id = frame_data["client_id"]
            stream_id = frame_data["stream_id"]
            width = frame_data["width"]
            height = frame_data["height"]
            frame = frame_data["frame"]

            # Detect actual frame dimensions
            frame_height, frame_width = frame.shape[:2]

            # Use header resolution if available, otherwise use detected frame size
            source_width = width if width else frame_width
            source_height = height if height else frame_height

            # Calculate display size maintaining aspect ratio with max 640px
            max_dimension = max(source_width, source_height)
            if max_dimension > 640:
                scale = 640 / max_dimension
                display_width = int(source_width * scale)
                display_height = int(source_height * scale)
            else:
                display_width = source_width
                display_height = source_height

            # Resize frame to display size if needed
            if frame_width != display_width or frame_height != display_height:
                frame = cv2.resize(frame, (display_width, display_height))

            # Track windows by client_id
            if client_id not in windows:
                res_info = f" ({source_width}x{source_height} -> {display_width}x{display_height})"
                LOG.info(
                    f"Opening new window for client {client_id} (stream_id={stream_id}){res_info}"
                )
                # OpenCV operations must run on main thread
                cv2.namedWindow(client_id, cv2.WINDOW_NORMAL)
                # Use display resolution for window size (maintains aspect ratio, max 640)
                cv2.resizeWindow(client_id, display_width, display_height)

            # Update client metadata
            client_meta = windows.setdefault(client_id, {})
            client_meta["last_seen"] = current_time
            client_meta["stream_id"] = stream_id

            cv2.setWindowTitle(client_id, f"Stream {stream_id}")
            # OpenCV operations must run on main thread
            cv2.imshow(client_id, frame)

    finally:
        # Cancel reader task if still running
        if not reader_task.done():
            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass

        LOG.info("Final window cleanup starting...")
        # All OpenCV cleanup must run on main thread
        for client_id in windows.keys():
            try:
                cv2.destroyWindow(client_id)
            except Exception as e:
                LOG.warning(f"Error destroying window {client_id}: {e}")
        windows.clear()
        cv2.destroyAllWindows()
        # Ensure all windows are closed
        cv2.waitKey(1)
        LOG.info("All windows closed.")


async def main():
    await display_frames(events.subscribe())


if __name__ == "__main__":
    os.environ["DATABRICKS_CONFIG_PROFILE"] = "E2-DOGFOOD"
    asyncio.run(main())
