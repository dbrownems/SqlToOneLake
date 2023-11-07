# SqlToOneLake

This is a sample of how to copy data from SQL Server to OneLake.  

Update the code for your connection string and query, and your target OneLake location and it will create a parquet file in the destination containing all the rows from your query.

It's built for SQL Server and OneLake, but should support other ADO.NET sources and ADLS Gen2 with trivial changes.

The row group size in the parquet file is configurable, and the parquet file is spooled to local disk, and then uploaded to OneLake in parallel.  In testing this provided much better throughput than writing the parquet file directly to OneLake.

