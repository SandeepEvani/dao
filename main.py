if __name__ == "__main__":
    from dao.dao import DAO

    dao = DAO("config.json")

    result = dao.write(data="Hello", destination="rs://bucket_name/key", arg1=1)

    print(result)
