from reggie_app_runner import caddy

if __name__ == "__main__":
    log_line = ' {"level":"info","ts":1760473445.142467,"msg":"shutdown complete","signal":"SIGINT","exit_code":0}'
    caddy = caddy.run(
        """

    :8080 {
        log {
            output stdout
        }
        respond "Hello, world!"
    }
    """,
    )
    caddy.wait()
