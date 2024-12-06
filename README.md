## Design and Concepts

The Data Access Object (later referenced as the DAO) will revolve under the concept of Datastore and Data Objects.
A Data Store refers to a complete or a segment of a storage layer which usually are Block storages, File systems or
Object storages. While working with complex projects, a Data store could be narrowed down to a specific schema of a database,
a specific prefix in an object storage, a specific directory in a file system or a specific partition of a block storage
and while working with easier projects, the data store could represent an entire storage layer


Every Data store requires an Interface class to perform the required actions such as the read and the write opertations 
on the data store.

We store Data Objects in the Data Store, Data objects could be database tables or files or objects. Each of the 
different data object would have an independent set of properties.
Hence we use a Data Object class to collectively store different data objects and the 
respective properties
