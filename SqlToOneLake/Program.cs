using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.Data.SqlClient;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using System.Net;

Console.WriteLine($"Starting {DateTime.Now}");
var constr = "Data Source=.;Initial Catalog=AdventureWorksDW2019;Integrated Security=True;TrustServerCertificate=true";
var sql = @"

select f.*
from FactInternetSales f
cross join generate_series(0,5000000/60368) s

";
var endpopint = $"https://msit-onelake.dfs.fabric.microsoft.com/";
var workspaceName = "dbrowne_Trident";
var folder = "/LH.lakehouse/Files/test";
var filename = "test.5m.parquet";

//endpopint = "https://dbrownedlwc.dfs.core.windows.net/";
//workspaceName = "datalake";
//folder = "/";
//filename = "test.5m.parquet";

var rowGroupSize = 500000;

var useTempFile = true;

var tfn = Path.GetTempFileName();
using var tempFile = new FileStream(tfn, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, 1024 * 64);//, FileOptions.DeleteOnClose);

//var createFileTask = CreateOneLakeFileAsync(endpopint, workspaceName, folder, filename);

using var con = new SqlConnection(constr);
con.Open();
var cmd = new SqlCommand(sql, con);
using var rdr = cmd.ExecuteReader();


await WriteDatareaderToParquet(rdr, tempFile, rowGroupSize);

Console.WriteLine($"Copying {tempFile.Position/1024/1024}MB to OneLake file {DateTime.Now}");

tempFile.Position = 0;
var fc = CreateOneLakeFileClient(endpopint, workspaceName, folder, filename);
tempFile.Close();

try
{
    await fc.UploadAsync(tfn, overwrite: true);
}
finally
{
    File.Delete(tfn);
}

//var buf = new byte[1024 * 1024 * 4];
//while (true)
//{
//    var br = tempFile.Read(buf, 0, buf.Length);
//    if (br == 0)
//        break;
//    file.Write(buf, 0, buf.Length);
//    Console.WriteLine($"Copied {tempFile.Position / 1024 / 1024}MB to OneLake file {DateTime.Now}");
//}

Console.WriteLine($"Copied {tempFile.Position / 1024 / 1024}MB to OneLake file {DateTime.Now}");


Console.WriteLine($"Complete {DateTime.Now}");




DataLakeServiceClient GetDataLakeServiceClient(string endpoint)
{
    DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient(
        new Uri(endpoint),
        new DefaultAzureCredential(new DefaultAzureCredentialOptions() { ExcludeInteractiveBrowserCredential = false }));

    return dataLakeServiceClient;
}


DataLakeFileClient CreateOneLakeFileClient(string endpopint, string workspaceName, string folder, string filename)
{
    var dataLakeServiceClient = GetDataLakeServiceClient(endpopint);
    var dataLakeFileSystemClient = dataLakeServiceClient.GetFileSystemClient(workspaceName);
    var dirClient = dataLakeFileSystemClient.GetDirectoryClient(folder);
    var fileClient = dirClient.GetFileClient(filename);
    return fileClient;
}
async Task<Stream> CreateOneLakeFileAsync(string endpopint, string workspaceName, string folder, string filename)
{
    var fileClient = CreateOneLakeFileClient(endpopint, workspaceName, folder, filename);   
    var file = await fileClient.OpenWriteAsync(overwrite:true);
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
   
}