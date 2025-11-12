from reggie_tools import clients, configs


def main():
    email = """
    Hi team,

    During today’s morning check at Store ID 3478, I noticed that the front display refrigerator seems to be running a bit warmer than usual. The digital thermometer shows a temperature of 47°F, which is higher than the acceptable range for dairy products. The backroom coolers are stable around 37°F, so it seems isolated to the front unit.

    The rest of the store conditions are normal—lighting, HVAC, and humidity levels are fine. I’ve placed a temporary “Do Not Use” sign on the affected unit until maintenance can inspect it.

    Thanks,
    Jamie – Store Operations
    """

    spark = clients.spark(configs.get())
    df = spark.createDataFrame([(email,)], ["email"])
    df = df.selectExpr(
        "ai_extract(email, array('store_id', 'temperature', 'sender')) AS extracted"
    )
    df.show(truncate=False)


if __name__ == "__main__":
    main()
