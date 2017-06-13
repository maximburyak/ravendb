using Sparrow.Json.Parsing;
using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client;
using System.Runtime.CompilerServices;
using Jint.Native.Object;
using Jint.Native;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public class BlittableOjectInstanceOperationScope
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ShouldFilterProperty(string property)
        {
            return property == Constants.Documents.Indexing.Fields.ReduceKeyFieldName ||
                   property == Constants.Documents.Indexing.Fields.DocumentIdFieldName ||
                   property == Constants.Documents.Metadata.Id ||
                   property == Constants.Documents.Metadata.Etag ||
                   property == Constants.Documents.Metadata.LastModified ||
                   property == Constants.Documents.Metadata.IndexScore ||
                   property == Constants.Documents.Metadata.ChangeVector ||
                   property == Constants.Documents.Metadata.Flags;
        }

        private static readonly List<string> InheritedProperties = new List<string>
        {
            "length",
            "Map",
            "Where",
            "RemoveWhere",
            "Remove"
        };

        private static string CreatePropertyKey(string key, string property)
        {
            if (string.IsNullOrEmpty(property))
                return key;

            return property + "." + key;
        }

        public static object ToBlittableValue(JsValue v, string propertyKey, bool recursiveCall, BlittableJsonToken? token = null, object originalValue=null)
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

                KeyValuePair<object, JsValue> property;
                if (token.HasValue && (
                    (token.Value & BlittableJsonToken.Float) == BlittableJsonToken.Float) ||
                    (token.Value & BlittableJsonToken.Float) == BlittableJsonToken.Integer)                    
                {                
                    // If the current value is exactly as the original value, we can return the original value before we made the JS conversion, 
                    // which will convert a Int64 to jsFloat.
                    var jsValue = property.Value;
                    if (jsValue.IsNumber() && Math.Abs(num - jsValue.AsNumber()) < double.Epsilon)
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
                var blittableArrayInstance = v.TryCast<BlittableObjectArrayInstance>();

                if (blittableArrayInstance != null)
                {
                    return blittableArrayInstance.Self;
                }

                var jsArray = v.AsArray();
                var array = new DynamicJsonArray();

                foreach (var property in jsArray.GetOwnProperties())
                {
                    if (InheritedProperties.Contains(property.Key))
                        continue;

                    var jsInstance = property.Value.Value;
                    if (jsInstance == null)
                        continue;

                    var ravenJToken = ToBlittableValue(jsInstance, propertyKey + "[" + property.Key + "]", recursiveCall);
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
                var blittableObjectInstance = v.TryCast<BlittableObjectInstance>();
                return ToBlittable(v.AsObject(), propertyKey, recursiveCall);
            }
            if (v.IsRegExp())
                return null;

            throw new NotSupportedException(v.Type.ToString());
        }

        public static DynamicJsonValue ToBlittable(ObjectInstance jsObject, string propertyKey = null, bool recursiveCall = false)
        {
            if (jsObject.Class == "Function")
            {
                // getting a Function instance here,
                // means that we couldn't evaluate it using Jint
                return null;
            }

            var obj = new DynamicJsonValue();
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
                    obj[property.Key] = ToBlittableValue(value, CreatePropertyKey(property.Key, propertyKey), recursive);
                }
            }
            return obj;
        }

    }
}
