using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UpdateStatisticsParserLib.Helpers
{
    public sealed class TypeMap
    {
        private static Dictionary<Type, SqlDbType> _typeMapper;
        public static Dictionary<Type, SqlDbType> TypeMapper
        {
            get
            {
                if (_typeMapper == null)
                {
                    _typeMapper = new Dictionary<Type, SqlDbType>();

                    _typeMapper[typeof(Int32)] = SqlDbType.Int;
                    _typeMapper[typeof(long)] = SqlDbType.BigInt;
                    _typeMapper[typeof(float)] = SqlDbType.Float;
                    _typeMapper[typeof(double)] = SqlDbType.Decimal;
                    _typeMapper[typeof(decimal)] = SqlDbType.Decimal;
                    _typeMapper[typeof(bool)] = SqlDbType.Bit;
                    _typeMapper[typeof(string)] = SqlDbType.VarChar;
                    _typeMapper[typeof(DateTime)] = SqlDbType.DateTime;
                    _typeMapper[typeof(DateTimeOffset)] = SqlDbType.DateTimeOffset;     
                }
                return _typeMapper;
            }
        }
    }
}
