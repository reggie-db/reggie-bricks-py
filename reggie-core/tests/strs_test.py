"""Quick checks for `reggie_core.strs.tokenize` behavior (moved from source)."""

from lfp_logging import logs

from reggie_core.strs import tokenize

LOG = logs.logger()

if __name__ == "__main__":
    LOG.info(list[str](tokenize("HelloWorld", "hello_world", "helloWorld")))
    LOG.info(
        list[str](
            tokenize(
                "HelloWorld",
                "hello_world",
                "helloWorld",
                non_alpha_numeric=False,
                camel_case=False,
            )
        )
    )
    LOG.warning(
        list[str](
            tokenize(
                "HelloWorld",
                "hello_world",
                "helloWorld",
                non_alpha_numeric=False,
                camel_case=False,
                lower=False,
            )
        )
    )
    print(
        list[str](
            tokenize(
                "HelloWorld",
                "hello_world",
                "helloWorld",
                non_alpha_numeric=False,
                camel_case=True,
                lower=False,
            )
        )
    )
