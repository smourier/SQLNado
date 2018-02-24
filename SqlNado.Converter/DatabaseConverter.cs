using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseSchemaReader;
using DatabaseSchemaReader.DataSchema;
using SqlNado.Utilities;

namespace SqlNado.Converter
{
    public class DatabaseConverter
    {
        public DatabaseConverter(string connectionString, string providerName, string outputDirectoryPath)
        {
            if (connectionString == null)
                throw new ArgumentNullException(nameof(connectionString));

            if (providerName == null)
                throw new ArgumentNullException(nameof(providerName));

            if (outputDirectoryPath == null)
                throw new ArgumentNullException(nameof(outputDirectoryPath));

            ConnectionString = connectionString;
            ProviderName = providerName;
            OutputDirectoryPath = outputDirectoryPath;
        }

        public string ConnectionString { get; }
        public string ProviderName { get; }
        public string OutputDirectoryPath { get; }

        protected virtual DatabaseReader CreateDatabaseReader()
        {
            if (Conversions.TryChangeType(ProviderName, out SqlType type))
                return new DatabaseReader(ConnectionString, type);

            return new DatabaseReader(ConnectionString, ProviderName);
        }

        public virtual void Convert()
        {
            using (var reader = CreateDatabaseReader())
            {
                var schema = reader.ReadAll();
                foreach (var table in schema.Tables)
                {
                    Console.WriteLine(table.Name);
                }
            }
        }
    }
}
