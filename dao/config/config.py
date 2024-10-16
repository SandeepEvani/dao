# config.py
# Creates a config object for DAO

from importlib import import_module
from json import load
from operator import call


class Config:

    def __init__(self, file_path: str):
        """

        :param file_path:
        """

        self._raw_confs = self._read_raw_config(file_path)

        self._infer_configs()

        ...

    def _read_raw_config(self, file_path: str):
        """

        :param file_path:
        :return:
        """
        try:
            return load(open(file_path))
        except Exception as error:
            print("Error reading the config file", error)

        ...

    def _validate_raw_json(self): ...

    def _infer_configs(self):
        """
        Used to loop through the config file and create a new config
        :return:
        """

        self._mutated_confs = {}
        self._dao_objects = {}

        for data_store in self.get_data_stores:
            interface_module = import_module(data_store["interface_class_location"])
            interface_class = getattr(interface_module, data_store["interface_class"])

            self._mutated_confs.update(
                {
                    data_store["interface_class"]: {
                        "interface_class": interface_class,
                        "defaults": data_store["default_configs"],
                    }
                }
            )

            self._dao_objects.update(
                {
                    data_store["interface_class"]: call(
                        interface_class, **data_store["default_configs"]
                    )
                }
            )

    @property
    def get_data_stores(self):
        return self._raw_confs.get("data_stores")

    @property
    def get_confs(self):
        return self._mutated_confs

    @property
    def get_dao_objects(self):
        return self._dao_objects

    def get_dao_classes(self):

        for key, value in self._mutated_confs.items():
            yield key, value["interface_class"]


if __name__ == "__main__":
    c = Config(r"C:\Evani\DAO\config.json")

    print(c)
