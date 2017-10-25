using System;
using System.Collections.Generic;
using System.Text;

namespace SqlNado
{
    public class SQLiteCreateTableOptions
    {
        public bool DeclareGuidAsString { get; set; }
        public string DeclareGuidAsStringFormat { get; set; }
    }
}
