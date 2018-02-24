using System;
using System.CodeDom.Compiler;
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

        public virtual void Convert(IndentedTextWriter writer)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));

            using (var reader = CreateDatabaseReader())
            {
                var schema = reader.ReadAll();
                foreach (var table in schema.Tables)
                {
                    string className = GetValidIdentifier(table.Name);
                    writer.WriteLine("public class " + className);
                    writer.WriteLine("{");
                    writer.Indent++;

                    writer.WriteLine("public " + className + "()");
                    writer.WriteLine("{");
                    writer.Indent++;
                    writer.Indent--;
                    writer.WriteLine("}");
                    writer.WriteLine();

                    foreach (var col in table.Columns.Where(c => !c.IsComputed))
                    {
                        string propertyName = GetValidIdentifier(col.Name);
                        writer.WriteLine("public " + col.DataType.NetDataType + " " + propertyName + " { get; set; }");
                    }

                    writer.Indent--;
                    writer.WriteLine("}");
                    writer.WriteLine();
                }
            }
        }
    }
}
