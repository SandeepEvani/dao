from dao import DAO
from catalog import catalog


if __name__ == "__main__":

    dao = DAO("data_stores.json")
    table = catalog.get("raw.customer")
    result = dao.write(data="Hello", data_object=table, arg1=1)

    print(result)
