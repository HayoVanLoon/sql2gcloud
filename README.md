# sql2gcloud
Transfer data from an SQL database into Google Cloud

This is a small command line application writes the result of an SQL query into a Google Cloud Storage file.

Some notes:

* The query must start with "SELECT"
* The created file inherits the access list of its bucket.
* By default, double tildes are used as column separators (i.e.: "~~")
* Your database handles the query, so you are free to use any features provided by your SQL dialect
* As writing occurs while iterating over the result set, memory usage should not be too dependent on table size. (to be tested)

### Example
The following will 
`sql2gcloud -d jdbc:mysql://127.0.0.1/bar -u myuser -p \"mypass\" -separator \"~~\" -dr com.mysql.jdbc.Driver gs://mybucket/some_file.txt SELECT \* FROM foo;`

## Configuration file
The command line can be shortened by using a configuration file. Values provided in this file need not be provided on the command line. Do note that the values provided in the file will be overwritten by their command line counterparts (if provided).

```
{
  "driver": "com.mysql.jdbc.Driver",
  "database": "jdbc:mysql://127.0.0.1:3306/my_database",
  "user": "my_user",
  "password": "my_password",
  "bucket": "foo",
  "file": "bar/bla.txt",
  "separator": "~~",
  "query": "SELECT * FROM foo;"
}
```

### Example (with configuration file)
`sql2gcloud -c config.json -u myuser -p \"mypass\" gs://mybucket/some_file.txt SELECT \* FROM foo;`

## Running it
The project uses SBT (simple build tool) for building (for more info: [http://www.scala-sbt.org]).
