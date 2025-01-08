
# DAO

Data Access Object or DAO is a convenience module to ease the data access from different data stores.

We register our data stores with the DAO, and DAO manages the read and write methods for us without us explicitly using the particular methods from the classes
## Design

The DAO revolves around the two entities, The ***Data Stores*** and the ***Data Classes***

A ***Data Store*** refers to a complete or a segment of a storage layer which usually are Block storages, File systems or Object storages. While working with complex projects, a Data store could be narrowed down to a specific schema of a database, a prefix in an object storage, a specific directory in a file system or a specific partition of a block storage and while working with simpler ones, the data store could represent the entire storage layer.

Every Data store has to be associated to an Interface class to perform read and write operations on the data store.

We store ***Data Objects*** in the Data Store, Data objects could be database tables or files or objects. A Data Object is bound to a Data Store and the associated Interface class is used to perform the necessary operations on the Data Object.
## Feedback

If you have any feedback, please reach out at evani.sandeep.sh@gmail.com

