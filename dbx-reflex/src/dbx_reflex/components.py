from typing import Any

import reflex as rx


class IntersectionObserver(rx.el.Div):
    """Reflex component wrapper for browser ``IntersectionObserver`` events."""

    root: rx.Var[str]
    root_margin: rx.Var[str]
    threshold: rx.Var[float]

    @classmethod
    def create(cls, *children, **props) -> rx.el.Div:
        """Create component props and inject an id when one is not supplied."""
        if "id" not in props:
            props["id"] = rx.vars.get_unique_variable_name()
        return super().create(*children, **props)

    def get_event_triggers(self) -> dict[str, Any]:
        """Register custom event trigger wiring for ``on_intersect``."""
        return {
            **super().get_event_triggers(),
            "on_intersect": lambda e0: [e0],
        }

    def _exclude_props(self) -> list[str]:
        return ["root", "root_margin", "threshold", "on_intersect"]

    def _get_imports(self) -> dict[str, list[rx.utils.imports.ImportVar]]:
        return rx.utils.imports.merge_imports(
            super()._get_imports(),
            {
                "react": [rx.utils.imports.ImportVar(tag="useEffect")],
            },
        )

    def _get_hooks(self) -> str | None:
        on_intersect = self.event_triggers.get("on_intersect")
        if on_intersect is None:
            return None
        if isinstance(on_intersect, rx.EventChain):
            on_intersect = rx.utils.format.wrap(
                rx.utils.format.format_prop(on_intersect).strip("{}"),
                "(",
            )
        script_props = dict(
            on_intersect=on_intersect,
            root=(
                f"document.querySelector({rx.utils.format.format_prop(self.root).strip('{}')})"
                if self.root is not None
                else "null"
            ),
            root_margin=rx.utils.format.format_prop(
                self.root_margin if self.root_margin is not None else "0px"
            ).strip("{}"),
            threshold=rx.cond(self.threshold is not None, self.threshold, 0.9),
            ref=self.get_ref(),
        )
        return (
            """
            useEffect(() => {
                const observer = new IntersectionObserver((entries) => {
                    entries.forEach((entry) => {
                        if (entry.isIntersecting) {
                            %(on_intersect)s(entry)
                        }
                    });
                }, {
                    root: %(root)s,
                    rootMargin: %(root_margin)s,
                    threshold: %(threshold)s,
                })
                if (%(ref)s.current) {
                    observer.observe(%(ref)s.current)
                    return () => observer.disconnect()
                }
            }, []);
            """
            % script_props
        )


class ReactMarkdown(rx.Component):
    """Reflex wrapper for the `react-markdown` React component."""

    library = "react-markdown@10.1.0"
    tag = "Markdown"
    is_default = True

    skip_html: rx.Var[bool]
    unwrap_disallowed: rx.Var[bool]
    remark_plugins: rx.Var[list[rx.Var | tuple[rx.Var, rx.Var]]]
    rehype_plugins: rx.Var[list[rx.Var | tuple[rx.Var, rx.Var]]]


def react_markdown(
    content: str | rx.Var[str],
    **props: Any,
) -> ReactMarkdown:
    """Render markdown text using `react-markdown`.

    Args:
        content: Markdown source text.
        **props: Props supported by `react-markdown` (for example `skip_html`).
            If `remark_plugins` is omitted, GFM is enabled by default.

    Returns:
        A configured `ReactMarkdown` component instance.
    """
    remark_plugins = list(props.pop("remark_plugins", []))
    if not remark_plugins:
        remark_plugins.append(rx.markdown.plugin.gfm)
    props["remark_plugins"] = remark_plugins
    return ReactMarkdown.create(content, **props)
