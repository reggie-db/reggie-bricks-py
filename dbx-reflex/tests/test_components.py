from dbx_reflex import ReactMarkdown, react_markdown


def test_react_markdown_component_metadata():
    assert ReactMarkdown.library == "react-markdown@10.1.0"
    assert ReactMarkdown.tag == "Markdown"
    assert ReactMarkdown.is_default is True


def test_react_markdown_factory_returns_component():
    component = react_markdown("# Hello")
    assert isinstance(component, ReactMarkdown)
