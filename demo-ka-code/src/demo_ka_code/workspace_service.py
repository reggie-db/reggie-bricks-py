import base64
import os
import time
from dataclasses import dataclass
from typing import Iterator, Callable

from databricks.sdk.errors.platform import BadRequest
from databricks.sdk.service.workspace import ExportFormat, ExportResponse, ObjectInfo

from reggie_core import paths
from reggie_tools import clients


def export(
    path: str = "/", filter: Callable[[ObjectInfo], bool] | None = None
) -> Iterator["WorkspaceExport"]:
    wc = clients.workspace_client()
    ws_objs = wc.workspace.list(path, recursive=True)
    for ws_obj in ws_objs:
        if filter and not filter(ws_obj):
            continue
        ws_path = ws_obj.path
        try:
            resp = wc.workspace.export(ws_path, format=ExportFormat.SOURCE)
        except BadRequest:
            continue
        yield WorkspaceExport(ws_obj, resp)


@dataclass
class WorkspaceExport:
    info: ObjectInfo
    response: ExportResponse

    def content_str(self) -> str | None:
        try:
            return base64.b64decode(self.response.content).decode("utf8")
        except UnicodeDecodeError:
            return None

    def filename(self) -> str:
        return os.path.basename(self.info.path)


if __name__ == "__main__":
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "FIELD-ENG-EAST")
    dir = paths.temp_dir() / "workspace_export" / str(int(time.time() * 1000))
    dir.mkdir(parents=True, exist_ok=True)
    for resp in export(
        "/Workspace/Users/reggie.pierce@databricks.com",
        filter=lambda obj: "race" in obj.path.lower(),
    ):
        content_str = resp.content_str()
        if content_str:
            file = dir / f"{resp.filename()}.txt"
            file.write_text(content_str)
            print(file)
