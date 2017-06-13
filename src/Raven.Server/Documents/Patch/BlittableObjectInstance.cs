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

namespace Raven.Server.Documents.Patch
{

    // base on Jint's ArrayInstance implementation
    
    public class BlittableObjectInstance : ObjectInstance
    {
        private BlittableJsonReaderObject _parent;

        public BlittableObjectInstance(Engine engine, BlittableJsonReaderObject parent) : base(engine)
        {
            _parent = parent;
        }

        public override PropertyDescriptor GetOwnProperty(string propertyName)
        {
            if (Properties.TryGetValue(propertyName, out PropertyDescriptor descriptor) == false)
            {
                descriptor = new BlittablePropertyDescriptor(Engine, this._parent, propertyName);
                Properties[propertyName] = descriptor;
            }
            return descriptor;
        }

        public class BlittablePropertyDescriptor : PropertyDescriptor
        {
            private Engine _engine;
            public readonly BlittableJsonReaderObject Self;
            private string _name;

            public BlittablePropertyDescriptor(Engine engine, BlittableJsonReaderObject parent, string name)
            {
                _engine = engine;
                Self = parent;
                _name = name;

                Get = new BlittableGetterFunctionInstance(engine, parent, name);
                Set = new BlittableSetterFunctionInstance(engine, parent, name);
            }

            public class BlittableGetterFunctionInstance : FunctionInstance
            {
                private BlittableJsonReaderObject parent;
                private string name;

                public BlittableGetterFunctionInstance(Engine engine, BlittableJsonReaderObject parent, string name) : base(engine, null, null, false)
                {
                    this.parent = parent;
                    this.name = name;
                }

                public override JsValue Call(JsValue thisObject, JsValue[] arguments)
                {
                    var propertyIndex = parent.GetPropertyIndex(name);

                    if (propertyIndex == -1)
                        return JsValue.Undefined;

                    var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

                    parent.GetPropertyByIndex(propertyIndex,ref propertyDetails);                                        

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
                            return new BlittableObjectInstance(Engine, (BlittableJsonReaderObject)propertyDetails.Value);                            
                        case BlittableJsonToken.StartArray:
                            return new BlittableObjectArrayInstance(Engine, (BlittableJsonReaderArray)propertyDetails.Value);
                        default:
                            throw new ArgumentOutOfRangeException(propertyDetails.Token.ToString());
                    }
                }
            }

            public class BlittableSetterFunctionInstance : FunctionInstance
            {
                private BlittableJsonReaderObject _parent;
                private string _name;
                
                public BlittableSetterFunctionInstance(Engine engine, BlittableJsonReaderObject parent, string name) : base(engine, null, null, false)
                {
                    this._parent = parent;
                    this._name = name;
                }

                public override JsValue Call(JsValue thisObject, JsValue[] arguments)
                {
                    var newVal = arguments[0];
                    if (newVal.TryCast<FunctionInstance>() != null)
                    {
                        throw new ArgumentException("Can't set a function to a blittable");
                    }

                    BlittableJsonToken? token = null;
                    object originalValue = null;
                    if (_parent.Modifications == null)
                        _parent.Modifications = new DynamicJsonValue();

                    var propertyIndex = _parent.GetPropertyIndex(_name);

                    
                    if (propertyIndex != -1)
                    {
                        // todo: instead of removal, implement something more appropriate like "substitutions"                        
                        var details = new BlittableJsonReaderObject.PropertyDetails();
                        _parent.GetPropertyByIndex(propertyIndex, ref details);
                        token = details.Token;
                        originalValue = details.Value;
                    }

                    // todo: not sure that string.Empty here works fine
                    _parent.Modifications[_name]= BlittableOjectInstanceOperationScope.ToBlittableValue(newVal, string.Empty, true, token, originalValue);

                    return Null.Instance;
                }
            }
        }
    }
}
