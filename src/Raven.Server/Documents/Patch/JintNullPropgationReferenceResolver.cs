﻿using System;
using Jint;
using Jint.Native;
using Jint.Runtime.Interop;
using Jint.Runtime.References;

namespace Raven.Server.Documents.Patch
{
    public abstract class JintNullPropgationReferenceResolver : IReferenceResolver
    {
        public virtual bool TryUnresolvableReference(Engine engine, Reference reference, out JsValue value)
        {
            value = Null.Instance;
            return true;
        }

        public virtual bool TryPropertyReference(Engine engine, Reference reference, ref JsValue value)
        {
            return value.IsNull() || value.IsUndefined();
        }

        public bool TryGetCallable(Engine engine, object callee, out JsValue value)
        {
            var @ref = callee as Reference;
            if (@ref != null && @ref.IsUnresolvableReference())
            {
                throw new MissingMethodException($"Could not locate refrence to the method: {@ref.GetReferencedName()}");
            }
            value = (JsValue)(new ClrFunctionInstance(engine, (thisObj, values) => thisObj));
            return true;
        }

        public bool CheckCoercible(JsValue value)
        {
            return true;
        }
    }
}
