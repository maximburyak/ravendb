using Jint.Native.Object;
using System;
using System.Collections.Generic;
using System.Text;
using Jint;
using Jint.Runtime.Descriptors;
using Sparrow.Json;
using Jint.Native.Function;
using Jint.Runtime.Environments;
using Jint.Native;
using Jint.Native.Array;
using Jint.Runtime;
using Sparrow.Json.Parsing;
using System.Linq;
using Sparrow.Collections;

namespace Raven.Server.Documents.Patch
{
    // base on Jint's ArrayInstance implementation

    public class BlittableObjectInstance : ObjectInstance
    {
        public string Id;
        public readonly BlittableJsonReaderObject Blittable;
        public List<(string name, bool isDeleted, JsValue value)> Modifications;

        public BlittableObjectInstance(Engine engine, BlittableJsonReaderObject parent) : base(engine)
        {
            Blittable = parent;
        }

        public override PropertyDescriptor GetOwnProperty(string propertyName)
        {
            if (Properties.TryGetValue(propertyName, out PropertyDescriptor descriptor) == false)
            {
                descriptor = new BlittablePropertyDescriptor(Engine, this, propertyName);
                Properties[propertyName] = descriptor;
            }
            return descriptor;
        }

        public override void RemoveOwnProperty(string p)
        {
            Modifications.Add((p,true, null));
            base.RemoveOwnProperty(p);
        }


        public class BlittablePropertyDescriptor : PropertyDescriptor
        {
            private Engine _engine;
            public readonly BlittableObjectInstance Self;
            private string _name;
            private JsValue LastKnownValue;

            public BlittablePropertyDescriptor(Engine engine, BlittableObjectInstance self, string name)
            {
                _engine = engine;
                Self = self;
                _name = name;

               // Get = new BlittableGetterFunctionInstance(engine, this, name);
                Set = new BlittableSetterFunctionInstance(engine, this, name);
                Writable = true;

            }

            private JsValue _get;
    //        private JsValue _set;

            public new JsValue Get {
                get
                {
                    if (_get == null)
                        _get = new BlittableGetterFunctionInstance(_engine, this, _name);
                    return _get;
                }
                set
                {
                    _get = value;
                }
            }

            public override JsValue Value
            {
                get
                {
                    LastKnownValue = GetValue();
                    return LastKnownValue;
                }
                set
                {
                    SetValue(value);
                }
            }

            private JsValue GetValue()
            {
                if (LastKnownValue != null)
                    return LastKnownValue;
                                
                foreach (var item in Self.Modifications)
                {
                    if (item.name == _name)
                    {
                        if (item.isDeleted)
                            return JsValue.Undefined;
                        return item.value;
                    }
                }                

                var propertyIndex = Self.Blittable.GetPropertyIndex(_name);
                if (propertyIndex == -1)
                    return JsValue.Undefined;

                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

                Self.Blittable.GetPropertyByIndex(propertyIndex, ref propertyDetails, true);

                switch (propertyDetails.Token & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        return JsValue.Null;
                    case BlittableJsonToken.Boolean:
                        return new JsValue((bool)propertyDetails.Value);

                    case BlittableJsonToken.Integer:
                        return new JsValue((long)propertyDetails.Value);
                    case BlittableJsonToken.Float:
                        return new JsValue((double)(LazyDoubleValue)propertyDetails.Value);
                    case BlittableJsonToken.String:
                        return new JsValue(((LazyStringValue)propertyDetails.Value).ToString());
                    case BlittableJsonToken.CompressedString:
                        return new JsValue(((LazyCompressedStringValue)propertyDetails.Value).ToString());
                    case BlittableJsonToken.StartObject:
                        return new BlittableObjectInstance(_engine, (BlittableJsonReaderObject)propertyDetails.Value);
                    case BlittableJsonToken.StartArray:
                        Enumerable = true; // todo: maybe this should be set earlier
                        return new BlittableObjectArrayInstance(_engine, (BlittableJsonReaderArray)propertyDetails.Value);
                    default:
                        throw new ArgumentOutOfRangeException(propertyDetails.Token.ToString());
                }
            }

            private void SetValue(JsValue newVal)
            {
                if (newVal.TryCast<FunctionInstance>() != null)
                {
                    throw new ArgumentException("Can't set a function to a blittable");
                }

                if (Self.Modifications == null)
                    Self.Modifications = new List<(string, bool, JsValue)>();

                //BlittableOjectInstanceOperationScope.ToBlittableValue(newVal, string.Empty, true, token, originalValue)
                // todo: not sure that string.Empty here works fine
                LastKnownValue = newVal;
                Enumerable = newVal.IsArray() || newVal.IsObject();
                Self.Modifications.Add((_name,false, newVal));
            }

            public class BlittableGetterFunctionInstance : FunctionInstance
            {
                private BlittablePropertyDescriptor _descriptor;
                private string _name;

                public BlittableGetterFunctionInstance(Engine engine, BlittablePropertyDescriptor descriptor, string name) : base(engine, null, null, false)
                {
                    this._descriptor = descriptor;
                    this._name = name;
                }

                public override JsValue Call(JsValue thisObject, JsValue[] arguments)
                {
                    return _descriptor.GetValue();
                }


            }

            public class BlittableSetterFunctionInstance : FunctionInstance
            {
                private BlittablePropertyDescriptor _descriptor;
                private string _name;

                public BlittableSetterFunctionInstance(Engine engine, BlittablePropertyDescriptor descriptor, string name) : base(engine, null, null, false)
                {
                    this._descriptor = descriptor;
                    this._name = name;
                }

                public override JsValue Call(JsValue thisObject, JsValue[] arguments)
                {
                    var newVal = arguments[0];
                    _descriptor.SetValue(newVal);

                    return Null.Instance;
                }
            }
        }
    }
}
