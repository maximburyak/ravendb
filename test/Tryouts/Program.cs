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
using Jint.Native.Array;
using Jint.Runtime.Descriptors;
using System.Collections.Generic;
using Jint.Native.Function;
using Jint.Runtime.Environments;
using Jint.Native;
using Jint.Native.Object;

namespace Tryouts
{
    public class InputsArray : ArrayInstance
    {
        public class DocPropertyDescriptor : PropertyDescriptor
        {
            private Engine _engine;
            private BlittableObjectInstance _document;

            public DocPropertyDescriptor(Engine engine, BlittableObjectInstance document)
            {
                _engine = engine;
                _document = document;
                Get = new DocPropertyGeter(engine, this, null, null, false);
            }
            public override JsValue Value { get => GetValue(); set => base.Value = value; }
            public JsValue GetValue()
            {
                return _document;
            }

            public class DocPropertyGeter : FunctionInstance
            {
                private DocPropertyDescriptor _descriptor;

                public DocPropertyGeter(Engine engine, DocPropertyDescriptor descriptor, string[] parameters, LexicalEnvironment scope, bool strict) : base(engine, null, null, false)
                {
                    this._descriptor = descriptor;
                }

                public override JsValue Call(JsValue thisObject, JsValue[] arguments)
                {
                    return _descriptor.GetValue();
                }
            }


        }

        //public override JsValue Get(string propertyName)
        //{
        //    return curVal.Value;
        //}
        private IEnumerable<BlittableObjectInstance> _docs;

        public InputsArray(Engine engine, IEnumerable<BlittableObjectInstance> docs) : base(engine)
        {
            this._docs = docs;
            FastAddProperty("forEach", new GetPropertiesFunctionInstance(Engine, _docs, null, null, false), true, false, true);

        }
        //public override bool HasOwnProperty(string p)
        //{
        //    return true;
        //}
        PropertyDescriptor curVal;
        //public override PropertyDescriptor GetOwnProperty(string propertyName)
        //{
        //    return curVal;
        //}
        public class CustomPropertyDescriptor : PropertyDescriptor
        {
            private BlittableObjectInstance _doc;

            public CustomPropertyDescriptor(Engine engine, BlittableObjectInstance doc)
            {
                _doc = doc;
                Get = new Getter(engine, doc, this, null, null, false);
            }

            public override JsValue Value { get => new JsValue(_doc); set => base.Value = value; }

            public class Getter : FunctionInstance
            {
                private CustomPropertyDescriptor _descriptor;
                private BlittableObjectInstance _doc;

                public Getter(Engine engine, BlittableObjectInstance doc, CustomPropertyDescriptor descriptor, string[] parameters, LexicalEnvironment scope, bool strict) : base(engine, parameters, scope, strict)
                {
                    _descriptor = descriptor;
                    _doc = doc;
                }

                public override JsValue Call(JsValue thisObject, JsValue[] arguments)
                {
                    return new JsValue(_doc);
                }
            }
        }

        public override IEnumerable<KeyValuePair<string, PropertyDescriptor>> GetOwnProperties()
        {
            foreach (var item in _docs)
            {
                curVal = new CustomPropertyDescriptor(Engine, item);
                curVal.Enumerable = true;
                yield return new KeyValuePair<string, PropertyDescriptor>(item.Id, curVal);
            }
        }

        public class GetPropertiesFunctionInstance : FunctionInstance
        {
            private IEnumerable<BlittableObjectInstance> _docs;

            public GetPropertiesFunctionInstance(Engine engine, IEnumerable<BlittableObjectInstance> docs, string[] parameters, LexicalEnvironment scope, bool strict) : base(engine, parameters, scope, strict)
            {
                this._docs = docs;
            }

            public override JsValue Call(JsValue thisObject, JsValue[] arguments)
            {
                var func = arguments[0].TryCast<FunctionInstance>();

                foreach (var item in _docs)
                {
                    func.Call(this, new[] { new JsValue(item) });
                }
                return JsValue.Null;
            }
        }
    }
    public class IterationTest : RavenTestBase
    {
        //  param[s].Name2='chaim'          
        private const string script = "function Evaluate (param ){ " +

            //            @"
            //var i=0;
            //for (var s in param){
            //        s.Name2='chaim';      

            //}" +
            @"param.forEach(function(a,b,c){
            a.Name='chaim';
});" +

"}";
        private DocumentStore store;
        private DocumentDatabase database;
        private DocumentPatcher patcher;
        private DynamicJsonValue doc;
        private BlittableJsonReaderObject blittable;
        private JsonOperationContext context;
        private int _iterationsCount;

        public IterationTest(int iterationsCount)
        {
            this._iterationsCount = iterationsCount;
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

            };       

            this.context = JsonOperationContext.ShortTermSingleUse();
            this.blittable = context.ReadObject(doc, "stuff");
        }

        private IEnumerable<BlittableObjectInstance> GetDocs(Engine engine)
        {
            for (int i = 0; i < _iterationsCount; i++)
            {                
                BlittableObjectInstance blittableObjectInstance = new BlittableObjectInstance(engine,
                    blittable
                );
                blittableObjectInstance.Id = i.ToString();
                yield return blittableObjectInstance;
                var resDynamic = BlittableOjectInstanceOperationScope.ToBlittable(blittableObjectInstance);
               // Console.WriteLine(resDynamic["Name2"]);
            }
        }

        [Fact]
        public void RunNew()
        {            

            var engine = new Engine();
            engine.Execute(script);
            var inputs = new InputsArray(engine, GetDocs(engine));
            engine.Invoke("Evaluate", inputs);            
        }       

        [Fact]
        public void RunOld()
        {
            //   using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                // var blittable = context.ReadObject(doc, "stuff");
                var engine = new Engine();
                engine.Execute(@"function Evaluate(value){
value.Name='Chaim';
return value;
}");

                for (int i = 0; i < _iterationsCount; i++)
                {
                    Raven.Server.Documents.Patch.PatcherOperationScope scope = new Raven.Server.Documents.Patch.PatcherOperationScope(database);

                    var jsObject = scope.ToJsObject(engine, new Document()
                    {
                        Data = blittable
                    });

                    var result = engine.Invoke("Evaluate", jsObject);


                    var dynamic = scope.ToBlittable(result.AsObject());
                }
              

                //  blittable = context.ReadObject(dynamic, "Evaluate");

            }
        }
    }
//    public class MyTestNew : RavenTestBase
//    {
//        private const string script = "function Evaluate (param ){ " +
//                              "param.Name='Chaim';" +
//            @"for (var i=0; i< 100; i++)
//param['Name'+i]=i.toString();
//" +
//                         //     "param.Things = ['a','b','c','d']; " +
//                           //   "param.OtherThings[2]=999;" +
//        "return param;" +
//          "                }";
//        private DocumentStore store;
//        private DocumentDatabase database;
//        private DocumentPatcher patcher;
//        private DynamicJsonValue doc;
//        private BlittableJsonReaderObject blittable;
//        private JsonOperationContext context;

//        public MyTestNew()
//        {
//            this.store = GetDocumentStore();
//            var task = this.GetDatabase(store.Database);
            
//            this.database = AsyncHelpers.RunSync(() => this.GetDatabase(store.Database));
//            this.patcher = new DocumentPatcher(database);

//            var otherArray = new DynamicJsonArray();
//            for (int i = 0; i < 10; i++)
//            {
//                otherArray.Add(i);
//            }

//            this.doc = new DynamicJsonValue()
//            {
//                ["Name"] = "Moshe",
//           //     ["Age"] = 17,
//               // ["OtherThings"] = otherArray

//            };

//            for (int i = 0; i < 100; i++)
//            {
//                otherArray = new DynamicJsonArray();
//                for (int j = 0; j < 100; j++)
//                {
//                    otherArray.Add(i);
//                }

//                doc["Name" + i] = otherArray;
//            }

//            this.context = JsonOperationContext.ShortTermSingleUse();
//            this.blittable = context.ReadObject(doc, "stuff");

//        }
//        [Fact]
//        public void RunNew()
//        {
//            //using (var context = JsonOperationContext.ShortTermSingleUse())
//            {                
                
//                var engine = new Engine();
//                engine.Execute(script);


//                BlittableObjectInstance blittableObjectInstance = new BlittableObjectInstance(engine, blittable);



//                engine.Invoke("Evaluate", blittableObjectInstance);

//                var dynamic = BlittableOjectInstanceOperationScope.ToBlittable(blittableObjectInstance);

//                //blittable = context.ReadObject(dynamic, "Evaluate");

//            }
//        }


//        [Fact]
//        public void RunOld()
//        {
//         //   using (var context = JsonOperationContext.ShortTermSingleUse())
//            {               
//               // var blittable = context.ReadObject(doc, "stuff");
//                var engine = new Engine();
//                engine.Execute(script);
//                PatcherOperationScope scope = new PatcherOperationScope(database);

//                var jsObject = scope.ToJsObject(engine, new Document()
//                {
//                    Data = blittable
//                });

//                var result = engine.Invoke("Evaluate", jsObject);
                               

//                var dynamic = scope.ToBlittable(result.AsObject());

//              //  blittable = context.ReadObject(dynamic, "Evaluate");

//            }
//        }
//    }
    public class Program
    {
        public class User
        {
            public string Name;
        }

        public static void Main(string[] args)
        {
            //var test = new MyTestNew();

            //var sp = Stopwatch.StartNew();
            //for(var i=0; i< 10000; i++)
            //{
            //    test.RunOld();
            //}

            //Console.WriteLine($"Old:{sp.ElapsedMilliseconds}");

            //sp.Restart();
            //for (var i = 0; i < 10000; i++)
            //{
            //    test.RunNew();
            //}
            //Console.WriteLine($"New:{sp.ElapsedMilliseconds}");


            for (int i = 0; i < 10; i++)
            {
                var test = new IterationTest(100_000);

                var sp = Stopwatch.StartNew();
                test.RunNew();
                Console.WriteLine($"new: {sp.ElapsedMilliseconds}");
                sp.Restart();
                test.RunOld();
                Console.WriteLine($"old: {sp.ElapsedMilliseconds}");
            }
        }

      

        
    }
}
