from dbx_tools import clients

if __name__ == "__main__":
    spark = clients.spark()
    df = spark.read.format("binaryFile").load(
        "/Volumes/reggie_pierce/invoice_pipeline_dev/files/B16387324.pdf"
    )
    row = df.collect()[0]
    print(type(row.content))
