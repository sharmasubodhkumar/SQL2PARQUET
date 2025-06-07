using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Parquet;
using Parquet.Data;

class Program
{
    static async Task Main()
    {
        // Connection string and query
        string connectionString = "Server=localhost;Database=YourDatabase;Trusted_Connection=True;TrustServerCertificate=True;";
        string query = "SELECT Id, Name, CreatedDate FROM YourTable";

        // Lists to store schema and data
        var fields = new List<DataField>();
        var columnsData = new List<List<object>>();

        // Execute SQL query and get schema/data
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync();
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    // Create schema based on reader
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        string fieldName = reader.GetName(i);
                        Type fieldType = reader.GetFieldType(i);
                        DataField field;

                        // Map SQL types to Parquet types
                        if (fieldType == typeof(int))
                            field = new DataField<int>(fieldName);
                        else if (fieldType == typeof(string))
                            field = new DataField<string>(fieldName);
                        else if (fieldType == typeof(DateTime))
                            field = new DataField<DateTime>(fieldName);
                        else if (fieldType == typeof(decimal))
                            field = new DataField<decimal>(fieldName);
                        else if (fieldType == typeof(bool))
                            field = new DataField<bool>(fieldName);
                        else
                            throw new NotSupportedException($"Type {fieldType} not supported for column {fieldName}");

                        fields.Add(field);
                        columnsData.Add(new List<object>());
                    }

                    // Collect data
                    while (await reader.ReadAsync())
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            columnsData[i].Add(reader.IsDBNull(i) ? null : reader[i]);
                        }
                    }
                }
            }
        }

        // Create Parquet schema
        var schema = new Schema(fields);

        // Create DataColumns
        var columns = new List<DataColumn>();
        for (int i = 0; i < fields.Count; i++)
        {
            var field = fields[i];
            var data = columnsData[i].ToArray();
            columns.Add(new DataColumn(field, data));
        }

        // Write to Parquet file
        using (var stream = File.OpenWrite("output.parquet"))
        {
            using (var writer = await ParquetWriter.CreateAsync(schema, stream))
            {
                using (var rowGroupWriter = writer.CreateRowGroup())
                {
                    foreach (var column in columns)
                    {
                        rowGroupWriter.WriteColumn(column);
                    }
                }
            }
        }

        Console.WriteLine("Parquet file created successfully.");
    }
}
