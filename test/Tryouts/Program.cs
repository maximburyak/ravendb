using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Documents.Patch;
using Jint;
using FastTests;
using Xunit;
using Raven.Client.Documents;
using Raven.Server.Documents;
using System.Threading.Tasks;
using Raven.Client.Util;
using System.Diagnostics;

namespace Tryouts
{

    public class MyTestNew : RavenTestBase
    {
        private const string script = "function Evaluate (param ){ " +
                              "param.Name='Chaim';" +
            @"for (var i=0; i< 100; i++)
param['Name'+i]=i.toString();
" +
                         //     "param.Things = ['a','b','c','d']; " +
                           //   "param.OtherThings[2]=999;" +
        "return param;" +
          "                }";
        private DocumentStore store;
        private DocumentDatabase database;
        private DocumentPatcher patcher;
        private DynamicJsonValue doc;
        private BlittableJsonReaderObject blittable;
        private JsonOperationContext context;

        public MyTestNew()
        {
            this.store = GetDocumentStore();
            var task = this.GetDatabase(store.Database);
            
            this.database = AsyncHelpers.RunSync(() => this.GetDatabase(store.Database));
            this.patcher = new DocumentPatcher(database);

            var otherArray = new DynamicJsonArray();
            for (int i = 0; i < 10; i++)
            {
                otherArray.Add(i);
            }

            this.doc = new DynamicJsonValue()
            {
                ["Name"] = "Moshe",
           //     ["Age"] = 17,
               // ["OtherThings"] = otherArray

            };

            for (int i = 0; i < 100; i++)
            {
                otherArray = new DynamicJsonArray();
                for (int j = 0; j < 100; j++)
                {
                    otherArray.Add(i);
                }

                doc["Name" + i] = otherArray;
            }

            this.context = JsonOperationContext.ShortTermSingleUse();
            this.blittable = context.ReadObject(doc, "stuff");

        }
        [Fact]
        public void RunNew()
        {
            //using (var context = JsonOperationContext.ShortTermSingleUse())
            {                
                
                var engine = new Engine();
                engine.Execute(script);


                BlittableObjectInstance blittableObjectInstance = new BlittableObjectInstance(engine, blittable);



                engine.Invoke("Evaluate", blittableObjectInstance);

                var dynamic = BlittableOjectInstanceOperationScope.ToBlittable(blittableObjectInstance);

                //blittable = context.ReadObject(dynamic, "Evaluate");

            }
        }


        [Fact]
        public void RunOld()
        {
         //   using (var context = JsonOperationContext.ShortTermSingleUse())
            {               
               // var blittable = context.ReadObject(doc, "stuff");
                var engine = new Engine();
                engine.Execute(script);
                PatcherOperationScope scope = new PatcherOperationScope(database);

                var jsObject = scope.ToJsObject(engine, new Document()
                {
                    Data = blittable
                });

                var result = engine.Invoke("Evaluate", jsObject);
                               

                var dynamic = scope.ToBlittable(result.AsObject());

              //  blittable = context.ReadObject(dynamic, "Evaluate");

            }
        }
    }
    public class Program
    {
        public class User
        {
            public string Name;
        }

        public static void Main(string[] args)
        {
            var test = new MyTestNew();

            var sp = Stopwatch.StartNew();
            for(var i=0; i< 10000; i++)
            {
                test.RunOld();
            }

            Console.WriteLine($"Old:{sp.ElapsedMilliseconds}");

            sp.Restart();
            for (var i = 0; i < 10000; i++)
            {
                test.RunNew();
            }
            Console.WriteLine($"New:{sp.ElapsedMilliseconds}");
        }

      

        
    }
}
