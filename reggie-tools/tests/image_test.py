import base64
import io
from PIL import Image
import re
import numpy as np
import cv2


def _b64_to_ndarray(val) -> np.ndarray:
    # terminal case
    if isinstance(val, (bytes, bytearray, memoryview)):
        raw = bytes(val)
        pil = Image.open(io.BytesIO(raw)).convert("RGB")
        print("image format:", pil.format)
        return cv2.cvtColor(np.array(pil), cv2.COLOR_RGB2BGR)

    # normalize stringlike
    if not isinstance(val, str):
        val = str(val)

    # strip data URI
    if val.startswith("data:"):
        val = val.split(",", 1)[1]

    # possible binary passed as ascii escaped
    # try to detect if val is actually base64
    cleaned = re.sub(r"\s+", "", val)
    missing = (-len(cleaned)) % 4
    if missing:
        cleaned += "=" * missing

    try:
        raw = base64.b64decode(cleaned, validate=True)
        return _b64_to_ndarray(raw)
    except Exception:
        # not valid base64, assume raw bytes encoded as text
        return _b64_to_ndarray(val.encode("latin1"))


# simple script test
if __name__ == "__main__":
    print("paste base64, then press enter")
    b64 = input().strip()

    try:
        arr = _b64_to_ndarray(b64)
        print("shape:", arr.shape)
        print("dtype:", arr.dtype)

        ok, buf = cv2.imencode(".jpg", arr)
        if ok:
            out_path = "decoded_test.jpg"
            with open(out_path, "wb") as f:
                f.write(buf.tobytes())
            print("wrote", out_path)
        else:
            print("encode failed")

    except Exception as e:
        print("error:", e)
