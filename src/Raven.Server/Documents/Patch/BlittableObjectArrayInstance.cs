using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Function;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;

using Sparrow.Json;
using Sparrow.Json.Parsing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Server.Documents.Patch
{
    public class BlittableObjectArrayInstance : ArrayInstance
    {
        public readonly BlittableJsonReaderArray Blittable;
        private readonly Engine _engine;
        private IDictionary<uint, PropertyDescriptor> _array = new MruPropertyCache2<uint, PropertyDescriptor>();
        public Dictionary<int, (bool isDeleted, int index, JsValue value)> Modifications;
        private PropertyDescriptor _length;

        public BlittableObjectArrayInstance(Engine engine, BlittableJsonReaderArray blittable) : base(engine)
        {
            Blittable = blittable;
            _engine = engine;
            SetOwnProperty("length", new PropertyDescriptor
            {
                Value = new JsValue(blittable.Length),
                Configurable = true,
                Enumerable = true,
                Writable = true,
            });

            Prototype = engine.Array.Prototype;
            SetOwnProperty("prototype", new PropertyDescriptor(Prototype, false, false, false));


            for (var i = 0; i < Blittable.Length; i++)
            {
                var indexAsString = i.ToString();
                BlittablePropertyDescriptor blittablePropertyDescriptor
                    = new BlittablePropertyDescriptor(Engine, this, i);
                SetOwnProperty(indexAsString, blittablePropertyDescriptor);
            }
        }

        //public static bool IsArrayIndex(JsValue p, out uint index)
        //{
        //    index = ParseArrayIndex(TypeConverter.ToString(p));

        //    return index != uint.MaxValue;

        //    // 15.4 - Use an optimized version of the specification
        //    // return TypeConverter.ToString(index) == TypeConverter.ToString(p) && index != uint.MaxValue;
        //}

        public void PersistModifications()
        {
            if (Modifications == null)
                return;

            if (Blittable.Modifications == null)
            {
                Blittable.Modifications = new DynamicJsonArray();
            }

            for (var i = 0; i < Blittable.Length; i++)
            {
                Blittable.Modifications.RemoveAt(i);
            }

            for (var i = 0; i < Blittable.Length; i++)
            {
                if (Modifications.TryGetValue(i, out var tuple))
                {
                    if (tuple.isDeleted)
                        continue;

                }
                else
                {
                    Blittable.Modifications.Add(Blittable[i]);
                }
            }

            if (_array.Count > Blittable.Length)
            {
                for (var i = Blittable.Length; i < _array.Count; i++)
                {
                    Blittable.Modifications.Add(Modifications[i].value);
                }
            }
        }

        public override string Class
        {
            get
            {
                return "Array";
            }
        }

        /// Implementation from ObjectInstance official specs as the one
        /// in ObjectInstance is optimized for the general case and wouldn't work
        /// for arrays
        private void Put(string propertyName, BlittablePropertyDescriptor valueDesc, bool throwOnError)
        {
            if (!CanPut(propertyName))
            {
                if (throwOnError)
                {
                    throw new JavaScriptException(Engine.TypeError);
                }

                return;
            }

            var ownDesc = GetOwnProperty(propertyName);

            if (ownDesc.IsDataDescriptor())
            {
                DefineOwnProperty(propertyName, valueDesc, throwOnError);
                return;
            }

            // property is an accessor or inherited
            var desc = GetProperty(propertyName);

            if (desc.IsAccessorDescriptor())
            {
                var setter = desc.Set.TryCast<ICallable>();
                setter.Call(new JsValue(this), new[] { valueDesc.Get.TryCast<ICallable>().Call(new JsValue(this), null) });
            }
            else
            {
                DefineOwnProperty(propertyName, valueDesc, throwOnError);
            }
        }
        /// Implementation from ObjectInstance official specs as the one
        /// in ObjectInstance is optimized for the general case and wouldn't work
        /// for arrays
        public override void Put(string propertyName, JsValue value, bool throwOnError)
        {
            if (!CanPut(propertyName))
            {
                if (throwOnError)
                {
                    throw new JavaScriptException(Engine.TypeError);
                }

                return;
            }

            var ownDesc = GetOwnProperty(propertyName);

            IsArrayIndex(propertyName, out var index);
            if (ownDesc.IsDataDescriptor())
            {
                var valueDesc = new BlittablePropertyDescriptor(Engine, this, (int)index);
                valueDesc.SetValue(value);
                DefineOwnProperty(propertyName, valueDesc, throwOnError);
                return;
            }

            // property is an accessor or inherited
            var desc = GetProperty(propertyName);

            if (desc.IsAccessorDescriptor())
            {
                var setter = desc.Set.TryCast<ICallable>();
                setter.Call(new JsValue(this), new[] { value });
            }
            else
            {
                var newDesc = new BlittablePropertyDescriptor(Engine, this, (int)index, value, true, true, true);
                DefineOwnProperty(propertyName, newDesc, throwOnError);
            }
        }

        public override bool DefineOwnProperty(string propertyName, PropertyDescriptor desc, bool throwOnError)
        {
            var oldLenDesc = GetOwnProperty("length");
            var oldLen = (uint)TypeConverter.ToNumber(oldLenDesc.Value);
            uint index;

            if (propertyName == "length")
            {
                if (desc.Value == null)
                {
                    return base.DefineOwnProperty("length", desc, throwOnError);
                }

                var newLenDesc = new PropertyDescriptor(desc);
                uint newLen = TypeConverter.ToUint32(desc.Value);
                if (newLen != TypeConverter.ToNumber(desc.Value))
                {
                    throw new JavaScriptException(_engine.RangeError);
                }

                newLenDesc.Value = newLen;
                if (newLen >= oldLen)
                {
                    return base.DefineOwnProperty("length", _length = newLenDesc, throwOnError);
                }
                if (!oldLenDesc.Writable.Value)
                {
                    if (throwOnError)
                    {
                        throw new JavaScriptException(_engine.TypeError);
                    }

                    return false;
                }
                bool newWritable;
                if (!newLenDesc.Writable.HasValue || newLenDesc.Writable.Value)
                {
                    newWritable = true;
                }
                else
                {
                    newWritable = false;
                    newLenDesc.Writable = true;
                }

                var succeeded = base.DefineOwnProperty("length", _length = newLenDesc, throwOnError);
                if (!succeeded)
                {
                    return false;
                }

                // in the case of sparse arrays, treat each concrete element instead of
                // iterating over all indexes

                if (_array.Count < oldLen - newLen)
                {
                    var keys = _array.Keys.ToArray();
                    foreach (var key in keys)
                    {
                        uint keyIndex;
                        // is it the index of the array
                        if (IsArrayIndex(key, out keyIndex) && keyIndex >= newLen && keyIndex < oldLen)
                        {
                            var deleteSucceeded = Delete(key.ToString(), false);
                            if (!deleteSucceeded)
                            {
                                newLenDesc.Value = new JsValue(keyIndex + 1);
                                if (!newWritable)
                                {
                                    newLenDesc.Writable = false;
                                }
                                base.DefineOwnProperty("length", _length = newLenDesc, false);

                                if (throwOnError)
                                {
                                    throw new JavaScriptException(_engine.TypeError);
                                }

                                return false;
                            }
                        }
                    }
                }
                else
                {
                    while (newLen < oldLen)
                    {
                        // algorithm as per the spec
                        oldLen--;
                        var deleteSucceeded = Delete(TypeConverter.ToString(oldLen), false);
                        if (!deleteSucceeded)
                        {
                            newLenDesc.Value = oldLen + 1;
                            if (!newWritable)
                            {
                                newLenDesc.Writable = false;
                            }
                            base.DefineOwnProperty("length", _length = newLenDesc, false);

                            if (throwOnError)
                            {
                                throw new JavaScriptException(_engine.TypeError);
                            }

                            return false;
                        }
                    }
                }
                if (!newWritable)
                {
                    DefineOwnProperty("length", new PropertyDescriptor(value: null, writable: false, enumerable: null, configurable: null), false);
                }
                return true;
            }
            else if (IsArrayIndex(propertyName, out index))
            {
                if (index >= oldLen && !oldLenDesc.Writable.Value)
                {
                    if (throwOnError)
                    {
                        throw new JavaScriptException(_engine.TypeError);
                    }

                    return false;
                }
                var succeeded = base.DefineOwnProperty(propertyName, desc, false);
                if (!succeeded)
                {
                    if (throwOnError)
                    {
                        throw new JavaScriptException(_engine.TypeError);
                    }

                    return false;
                }
                if (index >= oldLen)
                {
                    oldLenDesc.Value = index + 1;
                    base.DefineOwnProperty("length", _length = oldLenDesc, false);
                }

                return true;
            }

            return base.DefineOwnProperty(propertyName, desc, throwOnError);
        }

        private uint GetLength()
        {
            return TypeConverter.ToUint32(_length.Value);
        }

        public override IEnumerable<KeyValuePair<string, PropertyDescriptor>> GetOwnProperties()
        {
            foreach (var entry in _array)
            {
                yield return new KeyValuePair<string, PropertyDescriptor>(entry.Key.ToString(), entry.Value);
            }

            foreach (var entry in base.GetOwnProperties())
            {
                yield return entry;
            }
        }

        public override PropertyDescriptor GetOwnProperty(string propertyName)
        {
            uint index;
            if (IsArrayIndex(propertyName, out index))
            {
                PropertyDescriptor result;
                if (_array.TryGetValue(index, out result))
                {
                    return result;
                }
                else
                {
                    return PropertyDescriptor.Undefined;
                }
            }

            return base.GetOwnProperty(propertyName);
        }

        protected override void SetOwnProperty(string propertyName, PropertyDescriptor desc)
        {
            uint index;
            if (IsArrayIndex(propertyName, out index))
            {
                _array[index] = desc;
            }
            else
            {
                if (propertyName == "length")
                {
                    _length = desc;
                }

                base.SetOwnProperty(propertyName, desc);
            }
        }

        public override bool HasOwnProperty(string p)
        {
            uint index;
            if (IsArrayIndex(p, out index))
            {
                return index < GetLength();
            }

            return base.HasOwnProperty(p);
        }

        public override void RemoveOwnProperty(string p)
        {
            uint index;
            if (IsArrayIndex(p, out index))
            {
                _array.Remove(index);
                if (Modifications == null)
                    Modifications = new Dictionary<int, (bool isDeleted, int index, JsValue value)>();

                Modifications[(int)index] = (true, (int)index, null);
            }

            base.RemoveOwnProperty(p);
        }

        internal static uint ParseArrayIndex(string p)
        {
            int d = p[0] - '0';

            if (d < 0 || d > 9)
            {
                return uint.MaxValue;
            }

            if (d == 0 && p.Length > 1)
            {
                // If p is a number that start with '0' and is not '0' then
                // its ToString representation can't be the same a p. This is
                // not a valid array index. '01' !== ToString(ToUInt32('01'))
                // http://www.ecma-international.org/ecma-262/5.1/#sec-15.4

                return uint.MaxValue;
            }

            ulong result = (uint)d;

            for (int i = 1; i < p.Length; i++)
            {
                d = p[i] - '0';

                if (d < 0 || d > 9)
                {
                    return uint.MaxValue;
                }

                result = result * 10 + (uint)d;

                if (result >= uint.MaxValue)
                {
                    return uint.MaxValue;
                }
            }

            return (uint)result;
        }

        public class BlittablePropertyDescriptor : PropertyDescriptor
        {
            private Engine _engine;
            private int _index;
            public JsValue LastKnownValue { get; set; }
            private BlittableObjectArrayInstance _parent;

            public BlittablePropertyDescriptor(Engine engine, BlittableObjectArrayInstance parent, int index)
            {
                _engine = engine;
                _index = index;
                _parent = parent;
                // todo: cleanup code here, pretty sure we won't need the _get and _set fields                
                Get = new BlittableGetterFunctionInstance(engine, this, index);
                Set = new BlittableSetterFunctionInstance(engine, this, index);
                this.Writable = true;
            }

            public BlittablePropertyDescriptor(Engine engine, BlittableObjectArrayInstance parent, int index, JsValue value, bool? writable, bool? enumerable, bool? configurable) : base(value, writable, enumerable, configurable)
            {
                _parent = parent;
                Get = new BlittableGetterFunctionInstance(engine, this, index);
                Set = new BlittableSetterFunctionInstance(engine, this, index);
                this.Writable = true;
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

                if (_parent.Modifications != null && _parent.Modifications.TryGetValue(_index, out var val))
                {
                    if (val.isDeleted)
                        return JsValue.Undefined;
                    return val.value;
                }

                var valueTuple = _parent.Blittable.GetValueTokenTupleByIndex(_index);

                switch (valueTuple.Item2 & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        return JsValue.Null;
                    case BlittableJsonToken.Boolean:
                        return new JsValue((bool)valueTuple.Item1);

                    case BlittableJsonToken.Integer:
                        return new JsValue((long)valueTuple.Item1);
                    case BlittableJsonToken.Float:
                        return new JsValue((double)(LazyDoubleValue)valueTuple.Item1);
                    case BlittableJsonToken.String:
                        return new JsValue(((LazyStringValue)valueTuple.Item1).ToString());
                    case BlittableJsonToken.CompressedString:
                        return new JsValue(((LazyCompressedStringValue)valueTuple.Item1).ToString());

                    case BlittableJsonToken.StartObject:
                        return new BlittableObjectInstance(_engine, (BlittableJsonReaderObject)valueTuple.Item1);
                    case BlittableJsonToken.StartArray:
                        Enumerable = true;
                        return new BlittableObjectArrayInstance(_engine, (BlittableJsonReaderArray)valueTuple.Item1);
                    default:
                        return JsValue.Undefined;
                }
            }

            public void SetValue(JsValue newVal)
            {
                if (newVal.TryCast<FunctionInstance>() != null)
                {
                    throw new ArgumentException("Can't set a function to a blittable");
                }

                if (_parent.Modifications == null)
                    _parent.Modifications = new Dictionary<int, (bool isDeleted, int index, JsValue value)>();

                // todo: not sure that string.Empty here works fine
                //_parent.Modifications[_index] = (false, _index, BlittableOjectInstanceOperationScope.ToBlittableValue(newVal, string.Empty, true, token, originalValue));
                _parent.Modifications[_index] = (false, _index, newVal);
                Enumerable = newVal.IsArray() || newVal.IsObject();
                LastKnownValue = newVal;
            }

            public class BlittableGetterFunctionInstance : FunctionInstance
            {
                private int _index;
                private BlittablePropertyDescriptor _descriptor;

                public BlittableGetterFunctionInstance(Engine engine, BlittablePropertyDescriptor descriptor, int index) : base(engine, null, null, false)
                {
                    this._index = index;
                    _descriptor = descriptor;
                }

                public override JsValue Call(JsValue thisObject, JsValue[] arguments)
                {
                    if (_index == -1)
                        return JsValue.Undefined;
                    return _descriptor.GetValue();
                }
            }

            public class BlittableSetterFunctionInstance : FunctionInstance
            {
                private int _index;
                private BlittablePropertyDescriptor _descriptor;

                public BlittableSetterFunctionInstance(Engine engine, BlittablePropertyDescriptor descriptor, int index) : base(engine, null, null, false)
                {
                    _index = index;
                    _descriptor = descriptor;
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
