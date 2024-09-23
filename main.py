if __name__ == "__main__":
    from dao.dao import DAO

    dao = DAO("config.json")

    dao.write(data="Hello", destination="s3")
