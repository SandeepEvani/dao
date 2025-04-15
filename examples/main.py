from catalog import catalog

from src.dao.dao import dao

if __name__ == "__main__":

    dao.init("examples/data_stores.json", lazy=False)

    table = catalog.get("raw.customer")

    result = dao.write(data="Hello", data_object=table, arg1=1)
    result = dao.write(data="Hello", data_object=table, arg1=1, dao_interface_class="Hudi")
    result = dao.write(data="Hello", data_object=table, arg1=1, dao_interface_class="Delta")

    print(result)
