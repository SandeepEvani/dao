class S3:
    registration_prefix = "s3"

    def __init__(self, confs=None, **kwargs):
        print("Inited again and again")

    def write_anything_to_s3(self, data, data_object, **kwargs):
        print("Data is being written")

        print(data, data_object, kwargs)

        print("Finished Writing data")

    def write_something_to_s3(self, data, data_object, arg1):
        print("Data is being written from somthing func ")

        print(data, data_object, arg1)

        print("Finished Writing data")

    def write_something_to_s3_2(self, data, data_object, arg1, **kwargs):
        print("Data is being written from somthing_2 func ")

        print(data, data_object, arg1, **kwargs)

        print("Finished Writing data")
