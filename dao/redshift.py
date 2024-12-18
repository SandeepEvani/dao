# File: redshift_utils
# Description: Perform Redshift operations from pyspark and psycopg2

__author__ = "Sandeep Evani"
__copyright__ = "Copyright 2023, AllCargo"
__credits__ = ["Sandeep Evani"]
__license__ = "Proprietary"
__version__ = "1.0"
__maintainer__ = "Sandeep Evani"
__email__ = "evani.sandeep@ganitinc.com"
__status__ = "Production"

##########################################################################################

###########################################################################################


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
