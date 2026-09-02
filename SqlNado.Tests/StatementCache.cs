using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace SqlNado.Tests;

[TestClass]
public class StatementCache
{
    [TestMethod]
    public void PerCallBypass_DoesNotPool()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.EnableStatementsCache = true;
        db.ExecuteNonQuery("CREATE TABLE t (Id INTEGER PRIMARY KEY, V INTEGER)");
        db.ExecuteNonQuery("INSERT INTO t (Id, V) VALUES (1, 10)");

        var baseline = db.GetStatementsCacheEntries().Length;
        Assert.IsGreaterThan(0, baseline);

        var noCache = new SQLiteExecutionOptions { UseStatementsCache = false };
        db.ExecuteNonQuery("DELETE FROM t WHERE Id IN (1, 2, 3)", noCache);
        Assert.HasCount(baseline, db.GetStatementsCacheEntries());

        db.ExecuteNonQuery("DELETE FROM t WHERE Id IN (4, 5, 6)");
        Assert.IsGreaterThan(baseline, db.GetStatementsCacheEntries().Length);
    }

    [TestMethod]
    public void SetStatementCache_KeepsCacheWhenClearFalse()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.EnableStatementsCache = true;
        db.ExecuteNonQuery("CREATE TABLE t (Id INTEGER PRIMARY KEY)");
        db.ExecuteNonQuery("INSERT INTO t (Id) VALUES (1)");

        var kept = db.GetStatementsCacheEntries().Length;
        Assert.IsGreaterThan(0, kept);

        db.SetStatementCache(false, clear: false);
        Assert.HasCount(kept, db.GetStatementsCacheEntries());

        db.SetStatementCache(true, clear: true);
        Assert.IsEmpty(db.GetStatementsCacheEntries());
    }

    [TestMethod]
    public void Load_WithOptions_Bypasses()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.EnableStatementsCache = true;
        db.Save(new Item { Id = 1, Name = "a" });

        var baseline = db.GetStatementsCacheEntries().Length;
        _ = db.Load<Item>("WHERE Id IN (7, 8, 9)", new SQLiteLoadOptions(db) { UseStatementsCache = false }).ToList();
        Assert.HasCount(baseline, db.GetStatementsCacheEntries());
    }

    [TestMethod]
    public void SchemaDdl_Bypasses()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.Save(new Item { Id = 1, Name = "a" });

        var baseline = db.GetStatementsCacheEntries().Length;
        db.DeleteIndex("nope_ix");
        db.DeleteTable("nope_tbl", false);
        Assert.HasCount(baseline, db.GetStatementsCacheEntries());

        db.ExecuteNonQuery("DROP TABLE IF EXISTS nope_tbl2");
        Assert.IsGreaterThan(baseline, db.GetStatementsCacheEntries().Length);
    }

    [TestMethod]
    public void ErrorHandlerFromOptions_IsHonored()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.ExecuteNonQuery("CREATE TABLE u (Id INTEGER PRIMARY KEY)");
        db.ExecuteNonQuery("INSERT INTO u (Id) VALUES (1)");

        SQLiteError captured = null;
        var opts = new SQLiteExecutionOptions
        {
            ErrorHandler = e => { captured = e; return SQLiteOnErrorAction.Break; },
        };

        db.ExecuteNonQuery("INSERT INTO u (Id) VALUES (1)", opts);
        Assert.IsNotNull(captured);
    }

    [TestMethod]
    public void NoStatementsCacheMarker_BypassesAndIsImmutable()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.EnableStatementsCache = true;
        db.ExecuteNonQuery("CREATE TABLE t (Id INTEGER PRIMARY KEY)");

        var baseline = db.GetStatementsCacheEntries().Length;
        db.ExecuteNonQuery("DELETE FROM t WHERE Id IN (11, 22)", SQLiteExecutionOptions.NoStatementsCache);
        Assert.HasCount(baseline, db.GetStatementsCacheEntries());

        var threw = false;
        try
        {
            SQLiteExecutionOptions.NoStatementsCache.UseStatementsCache = true;
        }
        catch (InvalidOperationException)
        {
            threw = true;
        }
        Assert.IsTrue(threw);
        Assert.AreEqual(false, SQLiteExecutionOptions.NoStatementsCache.UseStatementsCache);
    }

    [TestMethod]
    public void OptionsNotBoundAsQueryParameter()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.ExecuteNonQuery("CREATE TABLE t (Id INTEGER PRIMARY KEY, V INTEGER)");
        var noCache = new SQLiteExecutionOptions { UseStatementsCache = false };

        // (sql, options) and (sql, options, queryArgs) must NOT bind the options object as a SQL parameter (SQLITE_RANGE)
        db.ExecuteNonQuery("INSERT INTO t (Id, V) VALUES (?, ?)", noCache, 50, 500);
        Assert.AreEqual(500, db.ExecuteScalar<int>("SELECT V FROM t WHERE Id = ?", noCache, 50));
    }

    private sealed class Item
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
}
