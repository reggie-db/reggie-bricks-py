import reflex as rx

config = rx.Config(
    app_name="dbx_reflex",
    app_module_import="dbx_reflex.app",
    plugins=[rx.plugins.sitemap.SitemapPlugin()],
)
