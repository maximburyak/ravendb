using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using System.Linq;
using Jurassic.Library;
using Jurassic;

namespace Raven.Server.Documents.Patch
{
    public class PatcherOperationScope : IDisposable
    {
        private readonly DocumentDatabase _database;
        
        public readonly DynamicJsonArray DebugInfo = new DynamicJsonArray();

        private static readonly List<string> InheritedProperties = new List<string>
        {
            "length",
            "Map",
            "Where",
            "RemoveWhere",
            "Remove"
        };

        private DocumentsOperationContext _context;

        public bool DebugMode { get; }

        public readonly PatchDebugActions DebugActions;

        public string CustomFunctions { get; set; }

        public int AdditionalStepsPerSize { get; set; }

        public int MaxSteps { get; set; }

        public int TotalScriptSteps;

        public object ActualPatchResult { get; set; }
        public object PatchObject;

        public PatcherOperationScope(DocumentDatabase database, bool debugMode = false)
        {
            _database = database;
            DebugMode = debugMode;
            if (DebugMode)
            {
                DebugActions = new PatchDebugActions();
            }
        }

        public PatcherOperationScope Initialize(DocumentsOperationContext context)
        {
            _context = context;

            return this;
        }

        public ObjectInstance ToJsObject(ScriptEngine engine, Document document)
        {
            var instance = ToJsObject(engine, document.Data);
            return ApplyMetadataIfNecessary(instance, document.Id, document.ChangeVector, document.LastModified, document.Flags, document.IndexScore);
        }

        public ObjectInstance ToJsObject(ScriptEngine engine, DocumentConflict document, string propertyName)
        {
            var instance = ToJsObject(engine, document.Doc);
            return ApplyMetadataIfNecessary(instance, document.Id, document.ChangeVector, document.LastModified, flags: null, indexScore: null);
        }

        private static ObjectInstance ApplyMetadataIfNecessary(ObjectInstance instance, LazyStringValue id, string changeVector, DateTime? lastModified, DocumentFlags? flags, double? indexScore)
        {
            var metadataValue = instance.GetPropertyValue(Constants.Documents.Metadata.Key);
            
            if (metadataValue == null || metadataValue is ArrayInstance)
                return instance;

            var metadata = metadataValue as ObjectInstance;

            if (changeVector != null)
                metadata.SetPropertyValue(Constants.Documents.Metadata.ChangeVector, changeVector, true);

            if (lastModified.HasValue && lastModified != default(DateTime))
                metadata.SetPropertyValue(Constants.Documents.Metadata.LastModified, lastModified.Value.GetDefaultRavenFormat(), true);

            if (flags.HasValue && flags != DocumentFlags.None)
                metadata.SetPropertyValue(Constants.Documents.Metadata.Flags, flags.Value.ToString(), true);

            if (id != null)
                metadata.SetPropertyValue(Constants.Documents.Metadata.Id, id.ToString(), true);

            if (indexScore.HasValue)
                metadata.SetPropertyValue(Constants.Documents.Metadata.IndexScore, indexScore, true);
            
            return instance;
        }

        private static ObjectInstance ToJsObject(ScriptEngine engine, BlittableJsonReaderObject json)
        {
            return new BlittableObjectInstance(engine, json);
        }

        private static string CreatePropertyKey(string key, string property)
        {
            if (string.IsNullOrEmpty(property))
                return key;

            return property + "." + key;
        }

        private void ToBlittableJsonReaderObject(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, ObjectInstance jsObject, string propertyKey = null,
            bool recursiveCall = false)
        {        
            writer.StartWriteObject();
            WriteRawObjectPropertiesToBlittable(writer, jsObject, propertyKey, recursiveCall);
            writer.WriteObjectEnd();
        }

        public void WriteRawObjectPropertiesToBlittable(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, ObjectInstance jsObject, string propertyKey = null,
            bool recursiveCall = false)
        {

            
            var blittableObjectInstance = jsObject as BlittableObjectInstance;

            if (blittableObjectInstance != null)
            {
                var properties = blittableObjectInstance.Properties.ToDictionary(x => x.Key.ToString(), x => x.Value);
                foreach (var propertyIndex in blittableObjectInstance.Blittable.GetPropertiesByInsertionOrder())
                {
                    var prop = new BlittableJsonReaderObject.PropertyDetails();

                    blittableObjectInstance.Blittable.GetPropertyByIndex(propertyIndex, ref prop);

                    if (blittableObjectInstance.Deletes.Contains(prop.Name))
                        continue;

                    writer.WritePropertyName(prop.Name);
                                        
                    if (properties.Remove(prop.Name, out var modifiedValue))
                    {                        
                        WriteJsonValue(prop.Name, modifiedValue);
                    }
                    else
                    {
                        writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                    }
                }

                foreach (var modificationKvp in properties ?? Enumerable.Empty<KeyValuePair<string, object>>())
                {
                    writer.WritePropertyName(modificationKvp.Key);
                    WriteJsonValue(modificationKvp.Key, modificationKvp.Value);
                }
            }
            else
            {
                var properties = jsObject.Properties.ToList();
                foreach (var property in properties)
                {
                    string propertyName = property.Key.ToString();
                    if (ShouldFilterProperty(propertyName))
                        continue;

                    var value = property.Value;
                    if (value == null)
                        continue;

                    if (value is RegExpInstance)
                        continue;

                    writer.WritePropertyName(propertyName);
                    WriteJsonValue(propertyName, value);
                }
            }

            void WriteJsonValue(string name, object value)
            {
                bool recursive = jsObject == value;
                if (recursiveCall && recursive)
                    writer.WriteValueNull();
                else
                {
                    ToBlittableJsonReaderValue(writer, value, CreatePropertyKey(name, propertyKey), recursive);
                }
            }
        }
        
        internal object ToJsValue(ScriptEngine jintEngine, BlittableJsonReaderObject.PropertyDetails propertyDetails)
        {
            switch (propertyDetails.Token & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.Null:
                    return Null.Value;
                case BlittableJsonToken.Boolean:
                    return (bool)propertyDetails.Value;
                case BlittableJsonToken.Integer:
                    return (long)propertyDetails.Value;
                case BlittableJsonToken.LazyNumber:
                    return (double)(LazyNumberValue)propertyDetails.Value;
                case BlittableJsonToken.String:
                    return ((LazyStringValue)propertyDetails.Value).ToString();
                case BlittableJsonToken.CompressedString:
                    return ((LazyCompressedStringValue)propertyDetails.Value).ToString();
                case BlittableJsonToken.StartObject:
                    return new BlittableObjectInstance(jintEngine, (BlittableJsonReaderObject)propertyDetails.Value);
                case BlittableJsonToken.StartArray:
                    //return new BlittableObjectArrayInstance(jintEngine, (BlittableJsonReaderArray)propertyDetails.Value);
                    return BlittableObjectInstance.CreateArrayInstanceBasedOnBlittableArray(jintEngine, propertyDetails.Value as BlittableJsonReaderArray);
                default:
                    throw new ArgumentOutOfRangeException(propertyDetails.Token.ToString());
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ShouldFilterProperty(string property)
        {
            return property == Constants.Documents.Indexing.Fields.ReduceKeyFieldName ||
                   property == Constants.Documents.Indexing.Fields.DocumentIdFieldName ||
                   property == Constants.Documents.Metadata.Id ||
                   property == Constants.Documents.Metadata.LastModified ||
                   property == Constants.Documents.Metadata.IndexScore ||
                   property == Constants.Documents.Metadata.ChangeVector ||
                   property == Constants.Documents.Metadata.Flags;
        }

        private void ToBlittableJsonReaderValue(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, object v, string propertyKey, bool recursiveCall)
        {
            
            var vType = v.GetType();
            var typeCode = Type.GetTypeCode(vType);

            switch (typeCode)
            {
                case TypeCode.Boolean:
                    writer.WriteValue((bool)v);
                    return;
                case TypeCode.String:
                    const string RavenDataByteArrayToBase64 = "raven-data:byte[];base64,";
                    var value = v.ToString();
                    if (value != null && value.StartsWith(RavenDataByteArrayToBase64))
                    {
                        value = value.Remove(0, RavenDataByteArrayToBase64.Length);
                        var byteArray = Convert.FromBase64String(value);
                        writer.WriteValue(Encoding.UTF8.GetString(byteArray));
                        return;
                    }
                    writer.WriteValue(value);
                    return;
                case TypeCode.Byte:
                    writer.WriteValue((byte)v);
                    return;
                case TypeCode.SByte:
                    writer.WriteValue((SByte)v);
                    return;
                case TypeCode.UInt16:                    
                case TypeCode.UInt32:
                case TypeCode.Int16:
                case TypeCode.Int32:
                    writer.WriteValue((int)v);
                    return;
                case TypeCode.UInt64:                
                case TypeCode.Int64:
                    writer.WriteValue((long)v);
                    break;
                case TypeCode.Decimal:
                    writer.WriteValue((decimal)v);
                    return;
                case TypeCode.Double:
                case TypeCode.Single:
                    writer.WriteValue((double)v);
                    return;
                case TypeCode.DateTime:
                    writer.WriteValue(((DateTime)v).ToString(Default.DateTimeFormatsToWrite));
                    return;                  
            }
                      
            
            
            if (v == Null.Value || v == Undefined.Value)
            {
                writer.WriteValueNull();
                return;
            }
            if (v is ArrayInstance)
            {
                var jsArray = v as ArrayInstance;
                writer.StartWriteArray();
                foreach (var property in jsArray.Properties)
                {
                    if (InheritedProperties.Contains(property.Key))
                        continue;

                    var jsInstance = property.Value;
                    if (jsInstance == null)
                        continue;

                    ToBlittableJsonReaderValue(writer, jsInstance, propertyKey + "[" + property.Key + "]",
                        recursiveCall);
                }
                writer.WriteArrayEnd();
                return;
            }           
            if (v is ObjectInstance)
            {
                ToBlittableJsonReaderObject(writer, v as ObjectInstance, propertyKey, recursiveCall);
                return;
            }
            if (v is RegExpInstance)
            {
                writer.WriteValueNull();
                return;
            }

            throw new NotSupportedException(v.GetType().ToString());
        }

        public DynamicJsonValue ToBlittable(ObjectInstance jsObject, string propertyKey = null, bool recursiveCall = false)
        {
            // to support static / instance calls. This is ugly, but the code will go away with Jurrasic anyway
            return ToBlittable2(jsObject, propertyKey, recursiveCall);
        }
        public static DynamicJsonValue ToBlittable2(ObjectInstance jsObject, string propertyKey = null, bool recursiveCall = false)
        {
            if (jsObject is FunctionInstance)
            {
                // getting a Function instance here,
                // means that we couldn't evaluate it using Jint
                return null;
            }

            var obj = new DynamicJsonValue();

            // todo: maybe treat modifications here?

            var blittableObjectInstance = jsObject as BlittableObjectInstance;

            if (blittableObjectInstance != null)
            {
                foreach (var propertyIndex in blittableObjectInstance.Blittable.GetPropertiesByInsertionOrder())
                {
                    var prop = new BlittableJsonReaderObject.PropertyDetails();

                    blittableObjectInstance.Blittable.GetPropertyByIndex(propertyIndex, ref prop);

                    if (blittableObjectInstance.Modifications != null && blittableObjectInstance.Modifications.TryGetValue(prop.Name, out var modification))
                    {
                        blittableObjectInstance.Modifications.Remove(prop.Name);
                        if (modification.IsDeleted)
                            continue;

                        obj[prop.Name] = ToBlittableValue2(modification.Value, CreatePropertyKey(prop.Name, propertyKey), jsObject == modification.Value, prop.Token, prop.Value);
                    }
                    else
                    {
                        obj[prop.Name] = prop.Value;
                    }
                }

                foreach (var modificationKvp in blittableObjectInstance.Modifications ?? Enumerable.Empty<KeyValuePair<string, (bool isDeleted, JsValue value)>>())
                {
                    var recursive = jsObject == modificationKvp.Value.value;
                    if (recursiveCall && recursive)
                        obj[modificationKvp.Key] = null;
                    else
                        obj[modificationKvp.Key] = ToBlittableValue2(modificationKvp.Value.value, CreatePropertyKey(modificationKvp.Key, propertyKey), jsObject == modificationKvp.Value.value);
                }
            }
            else
            {
                foreach (var property in jsObject.GetOwnProperties())
                {
                    if (ShouldFilterProperty(property.Key))
                        continue;

                    var value = property.Value.Value;
                    if (value == null)
                        continue;

                    if (value.IsRegExp())
                        continue;

                    var recursive = jsObject == value;
                    if (recursiveCall && recursive)
                        obj[property.Key] = null;
                    else
                    {
                        var propertyIndexInBlittable = blittableObjectInstance?.Blittable.GetPropertyIndex(property.Key) ?? -1;

                        if (propertyIndexInBlittable < 0)
                        {
                            obj[property.Key] = ToBlittableValue2(value, CreatePropertyKey(property.Key, propertyKey), recursive);
                        }
                        else
                        {
                            var prop = new BlittableJsonReaderObject.PropertyDetails();
                            blittableObjectInstance.Blittable.GetPropertyByIndex(propertyIndexInBlittable, ref prop, true);
                            obj[property.Key] = ToBlittableValue2(value, CreatePropertyKey(property.Key, propertyKey), recursive, prop.Token, prop.Value);
                        }
                    }
                }
            }

            return obj;
        }

        public object ToBlittableValue(object v, string propertyKey, bool recursiveCall, BlittableJsonToken? token = null, object originalValue = null)
        {
            // ugly and temporary
            return ToBlittableValue2(v, propertyKey, recursiveCall, token, originalValue);
        }
        public static object ToBlittableValue2(object v, string propertyKey, bool recursiveCall, BlittableJsonToken? token = null, object originalValue = null)
        {
            if (v.IsBoolean())
                return v.AsBoolean();

            if (v.IsString())
            {
                const string RavenDataByteArrayToBase64 = "raven-data:byte[];base64,";
                var valueAsObject = v.ToObject();
                var value = valueAsObject?.ToString();
                if (value != null && value.StartsWith(RavenDataByteArrayToBase64))
                {
                    value = value.Remove(0, RavenDataByteArrayToBase64.Length);
                    var byteArray = Convert.FromBase64String(value);
                    return Encoding.UTF8.GetString(byteArray);
                }
                return value;
            }

            if (v.IsNumber())
            {
                var num = v.AsNumber();

                if (originalValue != null && token.HasValue && (
                    (token.Value & BlittableJsonToken.LazyNumber) == BlittableJsonToken.LazyNumber ||
                    (token.Value & BlittableJsonToken.Integer) == BlittableJsonToken.Integer))
                {
                    // If the current value is exactly as the original value, we can return the original value before we made the JS conversion, 
                    // which will convert a Int64 to jsFloat.

                    double originalDouble;
                    if (originalValue is LazyNumberValue ldv)
                        originalDouble = ldv;
                    else
                        originalDouble = Convert.ToDouble(originalValue);

                    if (Math.Abs(num - originalDouble) < double.Epsilon)
                        return originalValue;

                    //We might have change the type of num from Integer to long in the script by design 
                    //Making sure the number isn't a real float before returning it as integer
                    if (originalValue is int && (Math.Abs(num - Math.Floor(num)) <= double.Epsilon || Math.Abs(num - Math.Ceiling(num)) <= double.Epsilon))
                        return (long)num;
                    return num; //float
                }

                // If we don't have the type, assume that if the number ending with ".0" it actually an integer.
                var integer = Math.Truncate(num);
                if (Math.Abs(num - integer) < double.Epsilon)
                    return (long)integer;
                return num;
            }
            if (v.IsNull() || v.IsUndefined())
                return null;

            if (v.IsArray())
            {
                var jsArray = v.AsArray();
                var array = new DynamicJsonArray();

                foreach (var property in jsArray.GetOwnProperties())
                {
                    if (InheritedProperties.Contains(property.Key))
                        continue;

                    var jsInstance = property.Value.Value;
                    if (jsInstance == null)
                        continue;

                    var ravenJToken = ToBlittableValue2(jsInstance, propertyKey + "[" + property.Key + "]", recursiveCall);

                    if (ravenJToken == null)
                        continue;

                    array.Add(ravenJToken);
                }

                return array;
            }
            if (v.IsDate())
            {
                return v.AsDate().ToDateTime();
            }
            if (v.IsObject())
            {
                return ToBlittable2(v.AsObject(), propertyKey, recursiveCall);
            }
            if (v.IsRegExp())
                return null;

            throw new NotSupportedException(v.Type.ToString());
        }

        public void Dispose()
        {
        }

        public virtual object LoadDocument(string documentId, ScriptEngine engine, ref int totalStatements)
        {
            if (_context == null)
                ThrowDocumentsOperationContextIsNotSet();

            var document = _database.DocumentsStorage.Get(_context, documentId);

            if (DebugMode)
                DebugActions.LoadDocument.Add(documentId);

            if (document == null)
                return Null.Value;

            totalStatements += (MaxSteps / 2 + (document.Data.Size * AdditionalStepsPerSize));
            var execution = new ExecutionConstraing(totalStatements);
            engine.OnLoopIterationCall = execution.OnLoopIteration;

            // TODO: Make sure to add Constants.Indexing.Fields.DocumentIdFieldName to document.Data
            return ToJsObject(engine, document.Data);
        }
        public class ExecutionConstraing
        {
            public ExecutionConstraing(long maxLoopIterations)
            {
                _maxLoopIterations = maxLoopIterations;
            }
            public int LoopIterations = 0;
            private readonly long _maxLoopIterations;

            public void OnLoopIteration()
            {
                LoopIterations++;
                if (LoopIterations == _maxLoopIterations)
                    ThrowExceededLoopIterations();
            }

            private void ThrowExceededLoopIterations()
            {
                throw new InvalidOperationException($"Javascript code exceeded a total of {_maxLoopIterations}");
            }
        }
        private static void ThrowDocumentsOperationContextIsNotSet()
        {
            throw new InvalidOperationException("Documents operation context is not set");
        }

        public virtual string PutDocument(string id, object document, object metadata, string changeVector, ScriptEngine engine)
        {
            if (_context == null)
                ThrowDocumentsOperationContextIsNotSet();

            if (document == null || document is ObjectInstance == false)
            {
                throw new InvalidOperationException(
                    $"Created document must be a valid object which is not null or empty. Document ID: '{id}'.");
            }

            var data = ToBlittable(document as ObjectInstance);
            if (metadata != null && metadata is ObjectInstance)
            {
                data["@metadata"] = ToBlittable(metadata as ObjectInstance);
            }
            var dataReader = _context.ReadObject(data, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            var put = _database.DocumentsStorage.Put(_context, id, _context.GetLazyString(changeVector), dataReader);

            if (DebugMode)
            {
                DebugActions.PutDocument.Add(new DynamicJsonValue
                {
                    ["Id"] = id,
                    ["ChangeVector"] = changeVector,
                    ["Data"] = dataReader
                });
            }

            return put.Id;
        }

        public virtual void DeleteDocument(string documentId)
        {
            throw new NotSupportedException("Deleting documents is not supported.");
        }
    }
}
