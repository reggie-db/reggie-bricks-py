import os
import json
import base64
import requests
import pandas as pd
import cv2

from reggie_tools import configs

DATABRICKS_URL = "https://adb-984752964297111.11.azuredatabricks.net/serving-endpoints/reggie-pierce-iot-ingest-object-detection/invocations"


def encode_image_base64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def score_model(databricks_token: str, image_path: str):
    image_b64 = encode_image_base64(image_path)

    df = pd.DataFrame(
        {
            "image_base64": [image_b64],
        }
    )

    payload = {"dataframe_split": df.to_dict(orient="split")}

    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }

    resp = requests.post(
        DATABRICKS_URL,
        headers=headers,
        data=json.dumps(payload),
        timeout=30,
    )

    if resp.status_code != 200:
        raise RuntimeError(f"request failed {resp.status_code} {resp.text}")

    return resp.json()


def annotate_image(image_path: str, result: dict, output_path: str | None = None):
    img = cv2.imread(image_path)

    predictions = result["predictions"][0]["json_result"]

    for box in predictions:
        x1 = int(box["x1"])
        y1 = int(box["y1"])
        x2 = int(box["x2"])
        y2 = int(box["y2"])
        label = box["label"]
        conf = box["confidence"]

        cv2.rectangle(img, (x1, y1), (x2, y2), (0, 255, 0), 2)
        cv2.putText(
            img,
            f"{label} {conf:.2f}",
            (x1, max(y1 - 5, 10)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            (0, 255, 0),
            1,
        )

    if output_path is None:
        output_path = "annotated.jpg"

    cv2.imwrite(output_path, img)
    return output_path


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        raise SystemExit("usage: python score_and_annotate.py <image_path>")

    image_path = sys.argv[1]
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "FIELD_ENG_EAST")
    config = configs.get()

    result = score_model(configs.token(config), image_path)
    out = annotate_image(image_path, result)

    print("saved", out)
