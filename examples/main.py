from catalog import catalog

from dao import dao

if __name__ == "__main__":

    dao.init("examples/data_stores.json", lazy=False)

    table = catalog.get("raw.customer")

    result = dao.write(data="Hello", data_object=table, arg1=1)

    print(result)
