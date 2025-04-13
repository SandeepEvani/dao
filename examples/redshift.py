class Redshift:

    def __init__(self, **kwargs): ...

    def write_dataframe_to_redshift_primary(
        self, data, destination, bucket_zone="allcargo-conf-zone", **kwargs
    ):
        """

        Used to create dimension tables directly from RAW_LAYER
        Args:
            data
            destination
            bucket_zone

        Returns: Table in Redshift
        """
        print("I'm running")

        return True
