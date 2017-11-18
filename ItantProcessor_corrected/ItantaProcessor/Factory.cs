/*
 * +----------------------------------------------------------------------------------------------+
 * The service provider factory
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using System;
using System.Collections.Generic;

namespace ItantProcessor
{
    internal class Factory<T>
    {
        private Factory() { }

        static readonly Dictionary<int, Func<T>> m_MapOfObjects
             = new Dictionary<int, Func<T>>();

        public static T Create(int id)
        {
            Func<T> constructor = null;
            if (m_MapOfObjects.TryGetValue(id, out constructor))
                return constructor();

            throw new ArgumentException("No type registered for this id");
        }

        public static bool HasId(int id)
        {
            return m_MapOfObjects.ContainsKey(id);
        }

        public static void Register(int id, Func<T> ctor)
        {
            m_MapOfObjects.Add(id, ctor);
        }

    }
}
