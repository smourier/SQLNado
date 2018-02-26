using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using DatabaseSchemaReader;
using DatabaseSchemaReader.DataSchema;
using SqlNado.Utilities;

namespace SqlNado.Converter
{
    public class DatabaseConverter
    {
        public DatabaseConverter(string connectionString, string providerName)
        {
            if (connectionString == null)
                throw new ArgumentNullException(nameof(connectionString));

            if (providerName == null)
                throw new ArgumentNullException(nameof(providerName));

            ConnectionString = connectionString;
            ProviderName = providerName;
        }

        public string ConnectionString { get; }
        public string ProviderName { get; }
        public DatabaseConverterOptions Options { get; set; }
        public string TargetNamespace { get; set; }

        protected virtual DatabaseReader CreateDatabaseReader()
        {
            if (Conversions.TryChangeType(ProviderName, out SqlType type))
                return new DatabaseReader(ConnectionString, type);

            return new DatabaseReader(ConnectionString, ProviderName);
        }

        public virtual string GetValidIdentifier(string text)
        {
            if (text == null)
                throw new ArgumentNullException(nameof(text));

            if (string.IsNullOrWhiteSpace(text))
                throw new ArgumentException(null, nameof(text));

            var sb = new StringBuilder(text.Length);
            if (IsValidIdentifierStart(text[0]))
            {
                sb.Append(text[0]);
            }
            else
            {
                sb.Append('_');
            }

            bool nextUpper = false;
            for (int i = 1; i < text.Length; i++)
            {
                if (IsValidIdentifierPart(text[i]))
                {
                    if (nextUpper)
                    {
                        sb.Append(Char.ToUpper(text[i], CultureInfo.CurrentCulture));
                        nextUpper = false;
                    }
                    else
                    {
                        sb.Append(text[i]);
                    }
                }
                else
                {
                    if (text[i] == ' ')
                    {
                        nextUpper = true;
                    }
                    else
                    {
                        //sb.Append('_');
                    }
                }
            }
            return sb.ToString();
        }

        public virtual bool IsValidIdentifierStart(char character)
        {
            if (character == '_')
                return true;

            switch (CharUnicodeInfo.GetUnicodeCategory(character))
            {
                case UnicodeCategory.UppercaseLetter:
                case UnicodeCategory.LowercaseLetter:
                case UnicodeCategory.TitlecaseLetter:
                case UnicodeCategory.ModifierLetter:
                case UnicodeCategory.OtherLetter:
                case UnicodeCategory.LetterNumber:
                    return true;

                default:
                    return false;
            }
        }

        public virtual bool IsValidIdentifierPart(char character)
        {
            switch (CharUnicodeInfo.GetUnicodeCategory(character))
            {
                case UnicodeCategory.UppercaseLetter:
                case UnicodeCategory.LowercaseLetter:
                case UnicodeCategory.TitlecaseLetter:
                case UnicodeCategory.ModifierLetter:
                case UnicodeCategory.LetterNumber:
                case UnicodeCategory.NonSpacingMark:
                case UnicodeCategory.SpacingCombiningMark:
                case UnicodeCategory.DecimalDigitNumber:
                case UnicodeCategory.ConnectorPunctuation:
                case UnicodeCategory.Format:
                    return true;

                default:
                    return false;
            }
        }

        public string Convert()
        {
            using (var writer = new StringWriter())
            {
                Convert(writer);
                return writer.ToString();
            }
        }

        public void Convert(TextWriter writer)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));

            if (!(writer is IndentedTextWriter itw))
            {
                itw = new IndentedTextWriter(writer);
            }
            Convert(itw);
        }

        private static string ToVerbatim(string text)
        {
            if (text == null)
                return "null";

            return "@\"" + text.Replace("\"", "\"\"") + "\"";
        }

        public virtual void Convert(IndentedTextWriter writer)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));

            using (var reader = CreateDatabaseReader())
            {
                var schema = reader.ReadAll();
                bool removeRowGuids = schema.Provider == "System.Data.SqlClient" && !Options.HasFlag(DatabaseConverterOptions.KeepRowguid);

                foreach (var table in schema.Tables)
                {
                    var tableAtts = new Dictionary<string, string>();
                    string className = GetValidIdentifier(table.Name);
                    if (className != table.Name)
                    {
                        tableAtts[nameof(SQLiteTableAttribute.Name)] = ToVerbatim(table.Name);
                    }

                    if (!string.IsNullOrWhiteSpace(table.SchemaOwner))
                    {
                        tableAtts[nameof(SQLiteTableAttribute.Schema)] = ToVerbatim(table.SchemaOwner);
                    }

                    if (tableAtts.Count > 0)
                    {
                        writer.WriteLine("[SQLiteTable(" + string.Join(", ", tableAtts.Select(a => a.Key + " = " + a.Value)) + ")]");
                    }

                    string baseClassName = null;
                    bool derived = Options.HasFlag(DatabaseConverterOptions.DeriveFromBaseObject);
                    if (derived)
                    {
                        baseClassName = " : SQLiteBaseObject";
                    }

                    writer.WriteLine("public class " + className + baseClassName);
                    writer.WriteLine("{");
                    writer.Indent++;

                    if (derived)
                    {
                        writer.WriteLine("public " + className + "(SQLiteDatabase db)");
                        writer.Indent++;
                        writer.WriteLine(": base(db)");
                        writer.Indent--;
                    }
                    else
                    {
                        writer.WriteLine("public " + className + "()");
                    }

                    writer.WriteLine("{");
                    writer.Indent++;
                    writer.Indent--;
                    writer.WriteLine("}");
                    writer.WriteLine();

                    var propertyNames = new List<string>();

                    foreach (var col in table.Columns.Where(c => !c.IsComputed && c.DataType != null))
                    {
                        if (removeRowGuids &&
                            string.Compare(col.Name, "rowguid", StringComparison.OrdinalIgnoreCase) == 0 &&
                            col.DataType.NetDataType == typeof(Guid).FullName)
                            continue;

                        var colAtts = new Dictionary<string, string>();
                        if (col.IsPrimaryKey)
                        {
                            colAtts[nameof(SQLiteColumnAttribute.IsPrimaryKey)] = true.ToString().ToLowerInvariant();
                        }

                        string propertyName = GetValidIdentifier(col.Name);
                        if (string.Compare(propertyName, className, StringComparison.OrdinalIgnoreCase) == 0)
                        {
                            int itest = 0;
                            const string suffix = "Property";
                            string baseName = propertyName + suffix;
                            while (propertyNames.Contains(baseName, StringComparer.OrdinalIgnoreCase))
                            {
                                itest++;
                                baseName = propertyName + suffix + itest;
                            }

                            propertyName = baseName;
                        }

                        propertyNames.Add(propertyName);

                        if (propertyName != col.Name)
                        {
                            colAtts[nameof(SQLiteColumnAttribute.Name)] = ToVerbatim(col.Name);
                        }

                        if (col.Nullable)
                        {
                            colAtts[nameof(SQLiteColumnAttribute.IsNullable)] = true.ToString().ToLowerInvariant();
                        }

                        if (col.IsAutoNumber)
                        {
                            colAtts[nameof(SQLiteColumnAttribute.AutoIncrements)] = true.ToString().ToLowerInvariant();
                        }

                        if (colAtts.Count > 0)
                        {
                            writer.WriteLine("[SQLiteColumn(" + string.Join(", ", colAtts.Select(a => a.Key + " = " + a.Value)) + ")]");
                        }

                        if (derived)
                        {
                            writer.WriteLine("public " + col.DataType.NetDataType + " " + propertyName + " { get => DictionaryObjectGetPropertyValue<" + col.DataType.NetDataType + ">(); set => DictionaryObjectSetPropertyValue(value); }");
                        }
                        else
                        {
                            writer.WriteLine("public " + col.DataType.NetDataType + " " + propertyName + " { get; set; }");
                        }
                    }

                    writer.Indent--;
                    writer.WriteLine("}");
                    writer.WriteLine();
                }
            }
        }
    }
}
