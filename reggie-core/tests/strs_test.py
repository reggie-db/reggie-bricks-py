from reggie_core.strs import tokenize

if __name__ == "__main__":
    print(list[str](tokenize("HelloWorld", "hello_world", "helloWorld")))
    print(
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
    print(
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
