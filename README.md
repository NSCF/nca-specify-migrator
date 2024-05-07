### Specify Migrator

A script for importing specimen data into Specify, so that we don't have to split large files into chunks. 

This is an alternative to using the Specify Workbench, if you're happy to skip all the verification stuff. It is intended for initial migrations of large databases. If migrating a small database of less than 10 000 records rather use the workbench. 

The script does require that taxa already exist in the taxon backbone otherwise the import will not proceed, so import and validate taxonomy first. Geography must also exist and match values in the data to be imported. Agents, localities, etc, are all created if they aren't found in the database similarly to what happens with the workbench. 

As the import may fail part way through, any collection objects already imported will be skipped. This is to allow an import to be rerun once the issue that caused failure has been corrected in the data. 