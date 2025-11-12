import pathlib

import fastapi_code_generator.__main__ as fastapi_code_generator_main

from scripts import projects

if __name__ == "__main__":
    template_dir = pathlib.Path(__file__).parent / "openapi_template"
    fastapi_code_generator_main.app(
        [
            "--input",
            str(pathlib.Path("~/Desktop/open-api.yaml").expanduser()),
            "--output",
            str(projects.root_dir() / "demo-iot/src/demo_iot_generated"),
            "--template-dir",
            str(template_dir),
        ],
    )
