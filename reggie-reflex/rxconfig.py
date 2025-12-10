import reflex as rx

config = rx.Config(
    app_name="reggie_reflex",
    app_module_import="reggie_reflex.app",
    plugins=[rx.plugins.TailwindV3Plugin()],
)
