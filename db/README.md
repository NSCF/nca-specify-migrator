Note that these are all interfaces for the tables in the database, not individual objects/records in those tables. Each one has methods to find, insert, or update records in the database.

The signature for each class constructor takes a connection object, and then any organisational struction fields required by the table (e.g. disciplineid for Taxon because taxa are limited to a discipline). 

When using the insert() methods on each class, it is expected that the calling script will have checked whether that records already exists in the database using find(), if relevant. 

In all cases insert() uses the raw database column names, not the mapped names in Specify.