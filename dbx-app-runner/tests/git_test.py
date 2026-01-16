from dbx_app_runner import git

if __name__ == "__main__":
    print(git.remote_commit_hash("https://github.com/reggie-db/reggie-bricks-py"))
    # clone("https://github.com/reggie-db/reggie-bricks-py", "./reggie-bricks-py")
