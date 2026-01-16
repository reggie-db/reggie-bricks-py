from dbx_tools import clients, configs, runtimes

if "__main__" == __name__:
    config = configs.get()
    spark = clients.spark(config)
    print(config.host)
    print(configs.token(config))
    print(runtimes.context(spark))
