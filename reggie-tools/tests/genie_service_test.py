import json
import os

from reggie_tools import clients

if __name__ == "__main__":
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "FIELD-ENG-EAST")
    wc = clients.workspace_client()

    resp = wc.files.download(
        "/Volumes/reggie_pierce/invoice_pipeline_dev/files/invoice_extraction.json"
    )
    raw_bytes = resp.contents.read()
    text = raw_bytes.decode("utf8")
    print(json.loads(text))
