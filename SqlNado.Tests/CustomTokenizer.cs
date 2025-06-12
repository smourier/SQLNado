using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlNado.Tests
{
    [TestClass]
    public class CustomTokenizer
    {
        [TestMethod]
        public void CustomTokenizer_tokenize()
        {
            using var db = new SQLiteDatabase(":memory:");
            using (var tok = db.GetTokenizer("unicode61", "remove_diacritics=0", "tokenchars=.=", "separators=X"))
            {
                Assert.AreEqual("hellofriends", string.Join(string.Empty, tok.Tokenize("hello friends")));
                GC.Collect(1000, GCCollectionMode.Forced, true);
            }

            var sp = new StopWordTokenizer(db);
            Assert.AreEqual(1, db.Configure(SQLiteDatabaseConfiguration.SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER, true, 1));

            db.SetTokenizer(sp);
            db.ExecuteNonQuery("CREATE VIRTUAL TABLE tok1 USING fts3tokenize('" + sp.Name + "');");

            //var tokens = db.LoadRows(@"SELECT token, start, end, position FROM tok1 WHERE input=?;",
            //    "This is a test sentence.");

            //Assert.AreEqual("testsentence", string.Join(string.Empty, tokens.Select(t => t["token"])));
        }

        [TestMethod]
        public void CustomTokenizer_tokenize_orm()
        {
            using var db = new SQLiteDatabase(":memory:");
            db.Configure(SQLiteDatabaseConfiguration.SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER, true, 1);
            var tok = new StopWordTokenizer(db);
            db.SetTokenizer(tok);

            // insert data as in SQLite exemple
            db.Save(new Mail { docid = 1, Subject = "software feedback", Body = "found it too slow" });
            db.Save(new Mail { docid = 2, Subject = "software feedback", Body = "no feedback" });
            db.Save(new Mail { docid = 3, Subject = "slow lunch order", Body = "was a software problem" });

            // check result
            Assert.AreEqual(3, db.LoadAll<Mail>().Count());

            Assert.AreEqual("1,2", string.Join(",", db.Load<Mail>("WHERE Subject MATCH 'software'").Select(m => m.docid)));
            Assert.AreEqual("2", string.Join(",", db.Load<Mail>("WHERE body MATCH 'feedback'").Select(m => m.docid)));
            Assert.AreEqual("1,2,3", string.Join(",", db.Load<Mail>("WHERE mail MATCH 'software'").Select(m => m.docid)));
            Assert.AreEqual("1,3", string.Join(",", db.Load<Mail>("WHERE mail MATCH 'slow'").Select(m => m.docid)));
            Assert.AreEqual("", string.Join(",", db.Load<Mail>("WHERE mail MATCH 'no'").Select(m => m.docid)));
        }
    }

    [SQLiteTable(Module = "fts3", ModuleArguments = nameof(Subject) + ", " + nameof(Body) + ", tokenize=" + StopWordTokenizer.TokenizerName)]
    public class Mail
    {
        public long docid { get; set; }
        public string Subject { get; set; }
        public string Body { get; set; }

        public override string ToString() => Subject + ":" + Body;
    }

    public class StopWordTokenizer(SQLiteDatabase database, params string[] arguments) : SQLiteTokenizer(database, TokenizerName)
    {
        public const string TokenizerName = "unicode_stopwords";

        private readonly static HashSet<string> _words;
        private readonly SQLiteTokenizer _unicode = database.GetUnicodeTokenizer(arguments);
        private int _disposed;

        static StopWordTokenizer()
        {
            _words = [];
            using var sr = new StringReader(_stopWords);
            do
            {
                var word = sr.ReadLine();
                if (word == null)
                    break;

                _words.Add(word);
            }
            while (true);
        }

        protected override void Dispose(bool disposing)
        {
            var disposed = Interlocked.Exchange(ref _disposed, 1);
            if (disposed != 0)
                return;

            if (disposing)
            {
                _unicode.Dispose();
            }

            base.Dispose(disposing);
        }

        public override IEnumerable<SQLiteToken> Tokenize(string input)
        {
            foreach (var token in _unicode.Tokenize(input))
            {
                // test native mangling stuff...
                GC.Collect(1000, GCCollectionMode.Forced, true);
                if (!_words.Contains(token.Text))
                {
                    yield return token;
                }
            }
        }

        // from https://raw.githubusercontent.com/mongodb/mongo/master/src/mongo/db/fts/stop_words_english.txt
        private const string _stopWords = @"a
about
above
after
again
against
all
am
an
and
any
are
aren't
as
at
be
because
been
before
being
below
between
both
but
by
can't
cannot
could
couldn't
did
didn't
do
does
doesn't
doing
don't
down
during
each
few
for
from
further
had
hadn't
has
hasn't
have
haven't
having
he
he'd
he'll
he's
her
here
here's
hers
herself
him
himself
his
how
how's
i
i'd
i'll
i'm
i've
if
in
into
is
isn't
it
it's
its
itself
let's
me
more
most
mustn't
my
myself
no
nor
not
of
off
on
once
only
or
other
ought
our
ours
ourselves
out
over
own
same
shan't
she
she'd
she'll
she's
should
shouldn't
so
some
such
than
that
that's
the
their
theirs
them
themselves
then
there
there's
these
they
they'd
they'll
they're
they've
this
those
through
to
too
under
until
up
very
was
wasn't
we
we'd
we'll
we're
we've
were
weren't
what
what's
when
when's
where
where's
which
while
who
who's
whom
why
why's
with
won't
would
wouldn't
you
you'd
you'll
you're
you've
your
yours
yourself
yourselves";
    }
}
