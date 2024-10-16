class S3:

    registration_prefix = "s3"

    def __init__(self, confs=None, **kwargs):
        print("Inited again and again")

    def write_anything_to_s3(self, data, destination, **kwargs):
        print("Data is being written")

        print(data, destination, kwargs)

        print("Finished Writing data")

    def write_something_to_s3(self, data, destination, arg1):
        print("Data is being written from somthing func ")

        print(data, destination, arg1)

        print("Finished Writing data")

    def write_something_to_s3_2(self, data, destination, arg1, **kwargs):
        print("Data is being written from somthing_2 func ")

        print(data, destination, arg1, **kwargs)

        print("Finished Writing data")
