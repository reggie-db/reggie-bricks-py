from dbx_tools import clients, configs

if "__main__" == __name__:
    config = configs.get()
    spark = clients.spark()
    print(config.host)
    print(configs.token())
    print("done")
    # print(runtimes.context(None))
