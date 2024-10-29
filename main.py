if __name__ == "__main__":
    from dao.dao import DAO

    dao = DAO("config.json")

    result = dao.write(
        data="Hello", destination="s3://bucket_name/key", dao_interface_class="Redshift"
    )

    print(result)
