from reggie_concurio import caches

if __name__ == "__main__":
    cache = caches.DiskCache(__file__)
    print(cache.directory)
    result = cache.get_or_load("test", loader=lambda: "this is a value", expire=10)
    print(result)
    print(result.load_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
