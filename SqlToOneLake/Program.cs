using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Microsoft.Data.SqlClient;
using Parquet;
using Parquet.Data;
using Parquet.Schema;


Console.WriteLine($"Starting {DateTime.Now}");
var constr = "Data Source=.;Initial Catalog=AdventureWorksDW2019;Integrated Security=True;TrustServerCertificate=true";
var sql = @"

select f.*
from FactInternetSales f
cross join generate_series(0,150000000/60368) s

";
var endpopint = $"https://msit-onelake.dfs.fabric.microsoft.com/";
var workspaceName = "dbrowne_Trident";
var folder = "/LH.lakehouse/Files/test";
var filename = "test.parquet";
var rowGroupSize = 1000000;

using Stream file = CreateOneLakeFile(endpopint, workspaceName, folder, filename);

using var con = new SqlConnection(constr);
con.Open();
var cmd = new SqlCommand(sql, con);
using var rdr = cmd.ExecuteReader();

await WriteDatareaderToParquet(rdr, file, rowGroupSize);

Console.WriteLine($"Complete {DateTime.Now}");




DataLakeServiceClient GetDataLakeServiceClient(string endpoint)
{
    DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient(
        new Uri(endpoint),
        new DefaultAzureCredential(new DefaultAzureCredentialOptions() { ExcludeInteractiveBrowserCredential = false }));

    return dataLakeServiceClient;
}

Stream CreateOneLakeFile(string endpopint, string workspaceName, string folder, string filename)
{
    var dataLakeServiceClient = GetDataLakeServiceClient(endpopint);
    var dataLakeFileSystemClient = dataLakeServiceClient.GetFileSystemClient(workspaceName);
    var dirClient = dataLakeFileSystemClient.GetDirectoryClient(folder);
    var fileClient = dirClient.GetFileClient(filename);
    var file = fileClient.OpenWrite(overwrite:true);
    return file;
}

static async Task WriteDatareaderToParquet(System.Data.IDataReader rdr, Stream file,  int rowGroupSize)
{
    var fields = new List<DataField>();
    var schemaTable = rdr.GetSchemaTable();
    if (schemaTable != null)
    {
        schemaTable.DefaultView.Sort = "ColumnOrdinal ASC";
        schemaTable = schemaTable.DefaultView.ToTable();
    }

    for (int i = 0; i < rdr.FieldCount; i++)
    {
        var field = rdr.GetName(i);
        var type = rdr.GetFieldType(i);

        var nullable = schemaTable == null ? true : (bool)schemaTable.Rows[i]["AllowDBNull"];

        var df = new DataField(field, type, isNullable: nullable);
        fields.Add(df);
    }

    
    var schema = new ParquetSchema(fields);

    var columns = new List<DataColumn>();
    foreach (var df in fields)
    {
        var dc = new DataColumn(df, Array.CreateInstance(df.ClrNullableIfHasNullsType, rowGroupSize));
        columns.Add(dc);
    }

    var opts = new ParquetOptions();
    var writer = await ParquetWriter.CreateAsync(schema, file, opts, false);

    writer.CompressionMethod = CompressionMethod.Snappy;

    int rc = 0;
    while (rdr.Read())
    {
        for (int i = 0; i < rdr.FieldCount; i++)
        {
            var value = rdr.GetValue(i);
            if (value == DBNull.Value)
                value = null;
            columns[i].Data.SetValue(value, rc);
        }
        rc++;
        if (rc % rowGroupSize == 0)
        {

            Console.WriteLine($"Writing {rc} rows. {DateTime.Now}");
            using var rgw = writer.CreateRowGroup();
            foreach (var c in columns)
            {
                Console.WriteLine($"Writing {c.Field.Name}");
                await rgw.WriteColumnAsync(c);
            }
            Console.WriteLine($"Completed {rc} rows. {DateTime.Now}");
            rc = 0;
        }
    }
    if (rc > 0)
    {
        for (int i = 0; i < columns.Count; i++)
        {
            var c = columns[i];
            var nc = new DataColumn(c.Field, Array.CreateInstance(c.Field.ClrNullableIfHasNullsType, rc));
            columns[i] = nc;
            for (int rn = 0; rn < rc; rn++)
            {
                nc.Data.SetValue(c.Data.GetValue(rn), rn);
            }

        }

        Console.WriteLine($"Writing {rc} rows.  Final Rowgroup.");
        using var rgw = writer.CreateRowGroup();
        foreach (var c in columns)
        {
            Console.WriteLine($"Writing {c.Field.Name}");
            await rgw.WriteColumnAsync(c);
        }
    }
    writer.Dispose();

    file.Flush();
    file.Close();
    
}