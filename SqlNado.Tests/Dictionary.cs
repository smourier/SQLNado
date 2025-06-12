using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlNado.Utilities;

namespace SqlNado.Tests
{
    [TestClass]
    public class Dictionary
    {
        [TestMethod]
        public void PersistentDictionary_create()
        {
            using var dic = new PersistentDictionary<string, int>();
            int max = 10;
            for (int i = 0; i < max; i++)
            {
                dic[i.ToString()] = i;
            }

            Assert.AreEqual(dic.Count, max);
            dic.Clear();
            Assert.AreEqual(dic.Count, 0);
        }

        [TestMethod]
        public void PersistentDictionary_typedCreated()
        {
            using var dic = new PersistentDictionary<string, object>();
            int max = 10;
            for (int i = 0; i < max; i++)
            {
                dic[i.ToString()] = i;
            }

            Assert.AreEqual(dic.Count, max);
            CollectionAssert.AllItemsAreInstancesOfType(dic.Values.ToArray(), typeof(int));

            dic.Clear();
            Assert.AreEqual(dic.Count, 0);
        }
    }
}
