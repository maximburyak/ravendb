using System;
using Jint.Native;
using Jint.Runtime.Interop;
using Sparrow;

namespace Raven.Server.Documents.Patch
{
    public class JintDateTimeConverter : IObjectConverter
    {
        public bool TryConvert(object value, out JsValue result)
        {
            if (value is DateTime dateTime)
            {
                result = (JsValue)(dateTime.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite));
                return true;
            }
            if (value is DateTimeOffset dateTimeOffset)
            {
                result = (JsValue)(dateTimeOffset.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite));
                return true;
            }

            result = null;
            return false;
        }
    }
}
