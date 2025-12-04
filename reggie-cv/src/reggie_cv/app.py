import hashlib
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar

import cv2
import imagehash
import numpy as np
from PIL import Image
from reggie_core import logs
from skimage.metrics import structural_similarity as ssim

T = TypeVar("T")

LOG = logs.logger(__file__)

rtsp_url = "rtsp://192.168.7.63:9000/live"

cap = cv2.VideoCapture(rtsp_url)

W = 1280
H = 720


def resize_for_display(frame: np.ndarray) -> np.ndarray:
    return cv2.resize(frame, (W, H))


@dataclass
class State(Generic[T]):
    """
    Represents the state of a fingerprint at a point in time.

    Attributes:
        value: The fingerprint/hash value of type T
        bounding_boxes: Optional list of bounding boxes for detected changes.
                        Each box is (x, y, width, height).
    """

    value: T
    bounding_boxes: list[tuple[int, int, int, int]] | None = None


class FingerprintService(ABC, Generic[T]):
    def __init__(self, name: str, min_interval: float = 1.0):
        self.name = name
        self.min_interval = min_interval
        self.last_state: State[T] | None = None
        self.last_change_ts = 0.0

    @abstractmethod
    def compute_state(self, frame: np.ndarray) -> State[T]:
        """
        Compute the fingerprint state from a frame, including bounding boxes if supported.
        """
        pass

    @abstractmethod
    def is_different(self, before: State[T], current: State[T]) -> bool:
        pass

    @abstractmethod
    def fingerprint_summary(self, state: State[T]) -> str:
        """
        Return a text summary/identifier for a fingerprint state.
        Used for logging purposes.
        """
        pass

    def has_changed(
        self, frame: np.ndarray, now: float
    ) -> tuple[bool, tuple[State[T], State[T]] | None]:
        """
        Check if the frame has changed. Detection runs at full FPS.
        Returns (changed, states) where states is (before_state, current_state) if changed.
        """
        current_state = self.compute_state(frame)

        if self.last_state is None:
            self.last_state = current_state
            self.last_change_ts = now
            return True, (current_state, current_state)

        if not self.is_different(self.last_state, current_state):
            self.last_state = current_state
            return False, None

        # Always update state for detection at full FPS
        before_state = self.last_state
        self.last_state = current_state

        # Only report change if enough time has passed since last report
        if now - self.last_change_ts < self.min_interval:
            return False, None

        self.last_change_ts = now
        return True, (before_state, current_state)


class HashService(FingerprintService[imagehash.ImageHash]):
    def __init__(
        self, name: str = "hash", min_interval: float = 1.0, threshold: int = 1
    ):
        super().__init__(name, min_interval)
        self.threshold = threshold

    def compute_state(self, frame: np.ndarray) -> State[imagehash.ImageHash]:
        """
        Compute hash fingerprint state. Hash service doesn't support bounding boxes.
        """
        fingerprint = imagehash.phash(Image.fromarray(frame))
        return State(value=fingerprint)

    def is_different(
        self, before: State[imagehash.ImageHash], current: State[imagehash.ImageHash]
    ) -> bool:
        diff = before.value - current.value
        self.last_diff = diff
        return diff > self.threshold

    def fingerprint_summary(self, state: State[imagehash.ImageHash]) -> str:
        """
        Return a string representation of the hash fingerprint.
        """
        return str(state.value)


class SSIMService(FingerprintService[np.ndarray]):
    def __init__(
        self, name: str = "ssim", min_interval: float = 1.0, threshold: float = 0.94
    ):
        super().__init__(name, min_interval)
        self.threshold = threshold
        self.last_score = None

    def compute_state(self, frame: np.ndarray) -> State[np.ndarray]:
        """
        Compute SSIM fingerprint state with bounding boxes for detected changes.
        """
        fingerprint = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        return State(value=fingerprint)

    def _compute_bounding_boxes(
        self, before: np.ndarray, current: np.ndarray
    ) -> list[tuple[int, int, int, int]]:
        """
        Compute bounding boxes for regions where changes are detected.
        Uses the service's SSIM threshold to determine significant differences.
        Returns list of (x, y, width, height) tuples.
        """
        # Compute SSIM with full=True to get difference map
        score, diff_map = ssim(before, current, full=True)

        # Convert difference map to uint8 and threshold based on service threshold
        # diff_map contains SSIM scores (0-1, where 1 is identical)
        # Convert to difference scale: (1 - SSIM) * 255
        diff_map = (1 - diff_map) * 255
        diff_map = diff_map.astype(np.uint8)

        # Threshold based on service's SSIM threshold
        # If threshold is 0.94, differences are where SSIM < 0.94
        # Convert to difference scale: (1 - 0.94) * 255 = 15.3
        diff_threshold = int((1 - self.threshold) * 255)
        _, thresh = cv2.threshold(diff_map, diff_threshold, 255, cv2.THRESH_BINARY)

        # Find contours
        contours, _ = cv2.findContours(
            thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
        )

        # Get bounding boxes
        boxes = []
        for contour in contours:
            x, y, w, h = cv2.boundingRect(contour)
            # Filter out very small boxes
            if w > 10 and h > 10:
                boxes.append((x, y, w, h))

        return boxes

    def is_different(
        self, before: State[np.ndarray], current: State[np.ndarray]
    ) -> bool:
        score = ssim(before.value, current.value)
        self.last_score = score

        if score < self.threshold:
            # Compute bounding boxes when difference is detected
            current.bounding_boxes = self._compute_bounding_boxes(
                before.value, current.value
            )
            return True
        return False

    def fingerprint_summary(self, state: State[np.ndarray]) -> str:
        """
        Return a hash-based identifier for the grayscale image fingerprint.
        """
        return hashlib.md5(state.value.tobytes()).hexdigest()[:8]


services = [
    HashService(),
    SSIMService(),
]

last_change_map = {}
change_counts = {svc.name: 0 for svc in services}

try:
    while True:
        ok, frame = cap.read()
        if not ok:
            continue

        # Display live feed at full FPS
        live_frame = resize_for_display(frame)
        cv2.imshow("Live Feed", live_frame)

        now = time.time()

        for svc in services:
            changed, states = svc.has_changed(frame, now)
            if changed:
                change_counts[svc.name] += 1
                last_change_map[svc.name] = svc.last_change_ts
                display_frame = resize_for_display(frame)

                # Draw bounding boxes if available
                if states and states[1].bounding_boxes:
                    for x, y, w, h in states[1].bounding_boxes:
                        # Scale bounding box coordinates to display size
                        scale_x = W / frame.shape[1]
                        scale_y = H / frame.shape[0]
                        x_scaled = int(x * scale_x)
                        y_scaled = int(y * scale_y)
                        w_scaled = int(w * scale_x)
                        h_scaled = int(h * scale_y)
                        cv2.rectangle(
                            display_frame,
                            (x_scaled, y_scaled),
                            (x_scaled + w_scaled, y_scaled + h_scaled),
                            (0, 255, 0),
                            2,
                        )

                cv2.imshow(svc.name, display_frame)

                before_summary = svc.fingerprint_summary(states[0])
                current_summary = svc.fingerprint_summary(states[1])
                box_info = (
                    f" ({len(states[1].bounding_boxes)} boxes)"
                    if states[1].bounding_boxes
                    else ""
                )
                score_info = ""
                if isinstance(svc, SSIMService) and svc.last_score is not None:
                    score_info = (
                        f" [SSIM: {svc.last_score:.4f}, threshold: {svc.threshold}]"
                    )
                LOG.info(
                    f"{svc.name} change detected (total: {change_counts[svc.name]}) "
                    f"{before_summary} -> {current_summary}{box_info}{score_info}"
                )

        if cv2.waitKey(1) == 27:
            break
except KeyboardInterrupt:
    LOG.info("Interrupted by user")
except Exception as e:
    LOG.error(f"Error occurred: {e}", exc_info=True)
finally:
    cap.release()
    cv2.destroyAllWindows()
