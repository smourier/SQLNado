﻿namespace SqlNado.Utilities;

public static class ConversionUtilities
{
    private static readonly char[] _enumSeparators = [',', ';', '+', '|', ' '];

    public static Type? GetEnumeratedType(Type collectionType)
    {
        if (collectionType == null)
            throw new ArgumentNullException(nameof(collectionType));

        var etype = GetEnumeratedItemType(collectionType);
        if (etype != null)
            return etype;

        foreach (Type type in collectionType.GetInterfaces())
        {
            etype = GetEnumeratedItemType(type);
            if (etype != null)
                return etype;
        }
        return null;
    }

    private static Type? GetEnumeratedItemType(Type type)
    {
        if (!type.IsGenericType)
            return null;

        if (type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            return type.GetGenericArguments()[0];

        if (type.GetGenericTypeDefinition() == typeof(ICollection<>))
            return type.GetGenericArguments()[0];

        if (type.GetGenericTypeDefinition() == typeof(IList<>))
            return type.GetGenericArguments()[0];

        return null;
    }

    public static double ToJulianDayNumbers(this DateTime date) => date.ToOADate() + 2415018.5;

    public static Guid ComputeGuidHash(string text)
    {
        if (text == null)
            throw new ArgumentNullException(nameof(text));

        using var md5 = MD5.Create();
        return new Guid(md5.ComputeHash(Encoding.UTF8.GetBytes(text)));
    }

    public static decimal ToDecimal(this byte[] bytes)
    {
        if (bytes == null || bytes.Length != 16)
            throw new ArgumentException(null, nameof(bytes));

        var ints = new int[4];
        Buffer.BlockCopy(bytes, 0, ints, 0, 16);
        return new decimal(ints);
    }

    public static byte[] ToBytes(this decimal dec)
    {
        var bytes = new byte[16];
        Buffer.BlockCopy(decimal.GetBits(dec), 0, bytes, 0, 16);
        return bytes;
    }

    public static byte[]? ToBytes(string? text)
    {
        if (text == null)
            return null;

        if (text.Length == 0)
            return [];

        var list = new List<byte>();
        var lo = false;
        byte prev = 0;
        int offset;

        // handle 0x or 0X notation
        if ((text.Length >= 2) && (text[0] == '0') && ((text[1] == 'x') || (text[1] == 'X')))
        {
            offset = 2;
        }
        else
        {
            offset = 0;
        }

        for (var i = 0; i < text.Length - offset; i++)
        {
            var b = GetHexaByte(text[i + offset]);
            if (b == 0xFF)
                continue;

            if (lo)
            {
                list.Add((byte)(prev * 16 + b));
            }
            else
            {
                prev = b;
            }
            lo = !lo;
        }

        return [.. list];
    }

    public static byte GetHexaByte(char c)
    {
        if (c >= '0' && c <= '9')
            return (byte)(c - '0');

        if (c >= 'A' && c <= 'F')
            return (byte)(c - 'A' + 10);

        if (c >= 'a' && c <= 'f')
            return (byte)(c - 'a' + 10);

        return 0xFF;
    }

    public static string? ToHexa(this byte[]? bytes) => ToHexa(bytes, 0, (bytes?.Length).GetValueOrDefault());
    public static string? ToHexa(this byte[]? bytes, int count) => ToHexa(bytes, 0, count);
    public static string? ToHexa(this byte[]? bytes, int offset, int count)
    {
        if (bytes == null)
            return null;

        if (offset < 0)
            throw new ArgumentException(null, nameof(offset));

        if (count < 0)
            throw new ArgumentException(null, nameof(count));

        if (offset > bytes.Length)
            throw new ArgumentException(null, nameof(offset));

        count = Math.Min(count, bytes.Length - offset);
        var sb = new StringBuilder(count * 2);
        for (var i = offset; i < (offset + count); i++)
        {
            sb.AppendFormat(CultureInfo.InvariantCulture, "X2", bytes[i]);
        }
        return sb.ToString();
    }

    public static string? ToHexaDump(string text, Encoding? encoding = null)
    {
        if (text == null)
            return null;

        encoding ??= Encoding.Unicode;

        return ToHexaDump(encoding.GetBytes(text));
    }

    public static string ToHexaDump(this byte[] bytes)
    {
        if (bytes == null)
            throw new ArgumentNullException(nameof(bytes));

        return ToHexaDump(bytes, null);
    }

    public static string ToHexaDump(this byte[] bytes, string? prefix)
    {
        if (bytes == null)
            throw new ArgumentNullException(nameof(bytes));

        return ToHexaDump(bytes, 0, bytes.Length, prefix, true);
    }

    public static string ToHexaDump(this IntPtr ptr, int count) => ToHexaDump(ptr, 0, count, null, true);
    public static string ToHexaDump(this IntPtr ptr, int offset, int count, string? prefix, bool addHeader)
    {
        if (ptr == IntPtr.Zero)
            throw new ArgumentNullException(nameof(ptr));

        var bytes = new byte[count];
        Marshal.Copy(ptr, bytes, offset, count);
        return ToHexaDump(bytes, 0, count, prefix, addHeader);
    }

    public static string ToHexaDump(this byte[] bytes, int count) => ToHexaDump(bytes, 0, count, null, true);
    public static string ToHexaDump(this byte[] bytes, int offset, int count) => ToHexaDump(bytes, offset, count, null, true);
    public static string ToHexaDump(this byte[] bytes, int offset, int count, string? prefix, bool addHeader)
    {
        if (bytes == null)
            throw new ArgumentNullException(nameof(bytes));

        if (offset < 0)
        {
            offset = 0;
        }

        if (count < 0)
        {
            count = bytes.Length;
        }

        if ((offset + count) > bytes.Length)
        {
            count = bytes.Length - offset;
        }

        var sb = new StringBuilder();
        if (addHeader)
        {
            sb.Append(prefix);
            //             0         1         2         3         4         5         6         7
            //             01234567890123456789012345678901234567890123456789012345678901234567890123456789
            sb.AppendLine("Offset    00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F  0123456789ABCDEF");
            sb.AppendLine("--------  -----------------------------------------------  ----------------");
        }

        for (int i = 0; i < count; i += 16)
        {
            sb.Append(prefix);
            sb.AppendFormat(CultureInfo.InvariantCulture, "{0:X8}  ", i + offset);

            int j;
            for (j = 0; (j < 16) && ((i + j) < count); j++)
            {
                sb.AppendFormat(CultureInfo.InvariantCulture, "{0:X2} ", bytes[i + j + offset]);
            }

            sb.Append(' ');
            if (j < 16)
            {
                sb.Append(new string(' ', 3 * (16 - j)));
            }
            for (j = 0; j < 16 && (i + j) < count; j++)
            {
                var b = bytes[i + j + offset];
                if (b > 31 && b < 128)
                {
                    sb.Append((char)b);
                }
                else
                {
                    sb.Append('.');
                }
            }

            sb.AppendLine();
        }
        return sb.ToString();
    }

    public static IList<T?> SplitToList<T>(string? text, params char[] separators) => SplitToList<T>(text, null, separators);
    public static IList<T?> SplitToList<T>(string? text, IFormatProvider? provider, params char[] separators)
    {
        var al = new List<T?>();
        if (text == null || separators == null || separators.Length == 0)
            return al;

        foreach (var s in text.Split(separators))
        {
            var value = s.Nullify();
            if (value == null)
                continue;

            var item = ChangeType(value, default(T), provider);
            al.Add(item);
        }
        return al;
    }

    public static bool EqualsIgnoreCase(this string? thisString, string? text, bool trim = false)
    {
        if (trim)
        {
            thisString = thisString.Nullify();
            text = text.Nullify();
        }

        if (thisString == null)
            return text == null;

        if (text == null)
            return false;

        if (thisString.Length != text.Length)
            return false;

        return string.Compare(thisString, text, StringComparison.OrdinalIgnoreCase) == 0;
    }

    public static string? Nullify(this string? text)
    {
        if (text == null)
            return null;

        if (string.IsNullOrWhiteSpace(text))
            return null;

        var t = text.Trim();
        return t.Length == 0 ? null : t;
    }

    public static Type? GetNullableTypeArgument(this Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        if (!IsNullable(type))
            return null;

        return type.GetGenericArguments()[0];
    }

    public static bool IsNullable(this Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
    }

    public static object? ChangeType(object? input, Type conversionType) => ChangeType(input, conversionType, null, null);
    public static object? ChangeType(object? input, Type conversionType, object? defaultValue) => ChangeType(input, conversionType, defaultValue, null);
    public static object? ChangeType(object? input, Type conversionType, object? defaultValue, IFormatProvider? provider)
    {
        if (!TryChangeType(input, conversionType, provider, out object? value))
            return defaultValue;

        return value;
    }

    public static T? ChangeType<T>(object? input) => ChangeType(input, default(T));
    public static T? ChangeType<T>(object? input, T? defaultValue) => ChangeType(input, defaultValue, null);
    public static T? ChangeType<T>(object? input, T? defaultValue, IFormatProvider? provider)
    {
        if (!TryChangeType(input, provider, out T? value))
            return defaultValue;

        return value;
    }

    public static bool TryChangeType<T>(object? input, out T? value) => TryChangeType(input, null, out value);
    public static bool TryChangeType<T>(object? input, IFormatProvider? provider, out T? value)
    {
        if (!TryChangeType(input, typeof(T), provider, out object? tvalue))
        {
            value = default;
            return false;
        }

        value = (T?)tvalue;
        return true;
    }

    public static bool TryChangeType(object? input, Type conversionType, out object? value) => TryChangeType(input, conversionType, null, out value);
    public static bool TryChangeType(object? input, Type conversionType, IFormatProvider? provider, out object? value)
    {
        if (conversionType == null)
            throw new ArgumentNullException(nameof(conversionType));

        if (conversionType == typeof(object))
        {
            value = input;
            return true;
        }

        if (conversionType.IsNullable())
        {
            Type nullableType = conversionType.GenericTypeArguments[0];
            if (input == null)
            {
                value = null;
                return true;
            }

            return TryChangeType(input, nullableType, provider, out value);
        }

        value = conversionType.IsValueType ? Activator.CreateInstance(conversionType) : null;
        if (input == null)
            return !conversionType.IsValueType;

        var inputType = input.GetType();
        if (conversionType.IsAssignableFrom(inputType))
        {
            value = input;
            return true;
        }

        if (conversionType.IsEnum)
            return EnumTryParse(conversionType, input, out value);

        if (conversionType == typeof(Guid))
        {
            if (0.Equals(input))
            {
                value = Guid.Empty;
                return true;
            }

            if (inputType == typeof(byte[]))
            {
                var bytes = (byte[])input;
                if (bytes.Length != 16)
                    return false;

                value = new Guid(bytes);
                return true;
            }

            var svalue = string.Format(provider, "{0}", input).Nullify();
            if (svalue != null && Guid.TryParse(svalue, out Guid guid))
            {
                value = guid;
                return true;
            }
            return false;
        }

        if (conversionType == typeof(IntPtr))
        {
            if (0.Equals(input))
            {
                value = IntPtr.Zero;
                return true;
            }

            if (IntPtr.Size == 8 && TryChangeType(input, provider, out long l))
            {
                value = new IntPtr(l);
                return true;
            }
            return false;
        }

        if (conversionType == typeof(bool))
        {
            if (0.Equals(input))
            {
                value = false;
                return true;
            }

            if (1.Equals(input))
            {
                value = true;
                return true;
            }

            if (inputType == typeof(byte[]))
            {
                var bytes = (byte[])input;
                if (bytes.Length != 1)
                    return false;

                value = BitConverter.ToBoolean(bytes, 0);
                return true;
            }
        }

        if (conversionType == typeof(int))
        {
            if (inputType == typeof(uint))
            {
                value = unchecked((int)(uint)input);
                return true;
            }

            if (inputType == typeof(ulong))
            {
                value = unchecked((int)(ulong)input);
                return true;
            }

            if (inputType == typeof(ushort))
            {
                value = unchecked((int)(ushort)input);
                return true;
            }

            if (inputType == typeof(byte))
            {
                value = unchecked((int)(byte)input);
                return true;
            }

            if (inputType == typeof(byte[]))
            {
                var bytes = (byte[])input;
                if (bytes.Length != 4)
                    return false;

                value = BitConverter.ToInt32(bytes, 0);
                return true;
            }
        }

        if (conversionType == typeof(long))
        {
            if (inputType == typeof(uint))
            {
                value = unchecked((long)(uint)input);
                return true;
            }

            if (inputType == typeof(ulong))
            {
                value = unchecked((long)(ulong)input);
                return true;
            }

            if (inputType == typeof(ushort))
            {
                value = unchecked((long)(ushort)input);
                return true;
            }

            if (inputType == typeof(byte))
            {
                value = unchecked((long)(byte)input);
                return true;
            }

            if (inputType == typeof(DateTime))
            {
                value = ((DateTime)input).Ticks;
                return true;
            }

            if (inputType == typeof(TimeSpan))
            {
                value = ((TimeSpan)input).Ticks;
                return true;
            }

            if (inputType == typeof(DateTimeOffset))
            {
                value = ((DateTimeOffset)input).Ticks;
                return true;
            }

            if (inputType == typeof(byte[]))
            {
                var bytes = (byte[])input;
                if (bytes.Length != 8)
                    return false;

                value = BitConverter.ToInt64(bytes, 0);
                return true;
            }
        }

        if (conversionType == typeof(short))
        {
            if (inputType == typeof(uint))
            {
                value = unchecked((short)(uint)input);
                return true;
            }

            if (inputType == typeof(ulong))
            {
                value = unchecked((short)(ulong)input);
                return true;
            }

            if (inputType == typeof(ushort))
            {
                value = unchecked((short)(ushort)input);
                return true;
            }

            if (inputType == typeof(byte))
            {
                value = unchecked((short)(byte)input);
                return true;
            }

            if (inputType == typeof(byte[]))
            {
                var bytes = (byte[])input;
                if (bytes.Length != 2)
                    return false;

                value = BitConverter.ToInt16(bytes, 0);
                return true;
            }
        }

        if (conversionType == typeof(sbyte))
        {
            if (inputType == typeof(uint))
            {
                value = unchecked((sbyte)(uint)input);
                return true;
            }

            if (inputType == typeof(ulong))
            {
                value = unchecked((sbyte)(ulong)input);
                return true;
            }

            if (inputType == typeof(ushort))
            {
                value = unchecked((sbyte)(ushort)input);
                return true;
            }

            if (inputType == typeof(byte))
            {
                value = unchecked((sbyte)(byte)input);
                return true;
            }

            if (inputType == typeof(byte[]))
            {
                var bytes = (byte[])input;
                if (bytes.Length != 1)
                    return false;

                value = unchecked((sbyte)bytes[0]);
                return true;
            }
        }

        if (conversionType == typeof(uint))
        {
            if (inputType == typeof(int))
            {
                value = unchecked((uint)(int)input);
                return true;
            }

            if (inputType == typeof(long))
            {
                value = unchecked((uint)(long)input);
                return true;
            }

            if (inputType == typeof(short))
            {
                value = unchecked((uint)(short)input);
                return true;
            }

            if (inputType == typeof(sbyte))
            {
                value = unchecked((uint)(sbyte)input);
                return true;
            }

            if (inputType == typeof(byte[]))
            {
                var bytes = (byte[])input;
                if (bytes.Length != 4)
                    return false;

                value = BitConverter.ToUInt32(bytes, 0);
                return true;
            }
        }

        if (conversionType == typeof(ulong))
        {
            if (inputType == typeof(int))
            {
                value = unchecked((ulong)(int)input);
                return true;
            }

            if (inputType == typeof(long))
            {
                value = unchecked((ulong)(long)input);
                return true;
            }

            if (inputType == typeof(short))
            {
                value = unchecked((ulong)(short)input);
                return true;
            }

            if (inputType == typeof(sbyte))
            {
                value = unchecked((ulong)(sbyte)input);
                return true;
            }

            if (inputType == typeof(byte[]))
            {
                var bytes = (byte[])input;
                if (bytes.Length != 8)
                    return false;

                value = BitConverter.ToUInt64(bytes, 0);
                return true;
            }
        }

        if (conversionType == typeof(ushort))
        {
            if (inputType == typeof(int))
            {
                value = unchecked((ushort)(int)input);
                return true;
            }

            if (inputType == typeof(long))
            {
                value = unchecked((ushort)(long)input);
                return true;
            }

            if (inputType == typeof(short))
            {
                value = unchecked((ushort)(short)input);
                return true;
            }

            if (inputType == typeof(sbyte))
            {
                value = unchecked((ushort)(sbyte)input);
                return true;
            }

            if (inputType == typeof(byte[]))
            {
                var bytes = (byte[])input;
                if (bytes.Length != 2)
                    return false;

                value = BitConverter.ToUInt16(bytes, 0);
                return true;
            }
        }

        if (conversionType == typeof(byte))
        {
            if (inputType == typeof(int))
            {
                value = unchecked((byte)(int)input);
                return true;
            }

            if (inputType == typeof(long))
            {
                value = unchecked((byte)(long)input);
                return true;
            }

            if (inputType == typeof(short))
            {
                value = unchecked((byte)(short)input);
                return true;
            }

            if (inputType == typeof(sbyte))
            {
                value = unchecked((byte)(sbyte)input);
                return true;
            }

            if (inputType == typeof(byte[]))
            {
                var bytes = (byte[])input;
                if (bytes.Length != 1)
                    return false;

                value = bytes[0];
                return true;
            }
        }

        if (conversionType == typeof(decimal) && inputType == typeof(byte[]))
        {
            var bytes = (byte[])input;
            if (bytes.Length != 16)
                return false;

            value = ToDecimal(bytes);
            return true;
        }

        if (conversionType == typeof(DateTime))
        {
            if (0.Equals(input))
            {
                value = DateTime.MinValue;
                return true;
            }

            if (inputType == typeof(long))
            {
                value = new DateTime((long)input);
                return true;
            }

            if (inputType == typeof(DateTimeOffset))
            {
                value = ((DateTimeOffset)input).DateTime;
                return true;
            }
        }

        if (conversionType == typeof(TimeSpan))
        {
            if (0.Equals(input))
            {
                value = TimeSpan.Zero;
                return true;
            }

            if (inputType == typeof(long))
            {
                value = new TimeSpan((long)input);
                return true;
            }
        }

        if (conversionType == typeof(char) && inputType == typeof(byte[]))
        {
            var bytes = (byte[])input;
            if (bytes.Length != 2)
                return false;

            value = BitConverter.ToChar(bytes, 0);
            return true;
        }

        if (conversionType == typeof(float) && inputType == typeof(byte[]))
        {
            var bytes = (byte[])input;
            if (bytes.Length != 4)
                return false;

            value = BitConverter.ToSingle(bytes, 0);
            return true;
        }

        if (conversionType == typeof(double) && inputType == typeof(byte[]))
        {
            var bytes = (byte[])input;
            if (bytes.Length != 8)
                return false;

            value = BitConverter.ToDouble(bytes, 0);
            return true;
        }

        if (conversionType == typeof(DateTimeOffset))
        {
            if (0.Equals(input))
            {
                value = DateTimeOffset.MinValue;
                return true;
            }

            if (inputType == typeof(DateTime))
            {
                value = new DateTimeOffset((DateTime)input);
                return true;
            }

            if (inputType == typeof(long))
            {
                value = new DateTimeOffset(new DateTime((long)input));
                return true;
            }
        }

        if (conversionType == typeof(byte[]))
        {
            if (inputType == typeof(int))
            {
                value = BitConverter.GetBytes((int)input);
                return true;
            }

            if (inputType == typeof(long))
            {
                value = BitConverter.GetBytes((long)input);
                return true;
            }

            if (inputType == typeof(short))
            {
                value = BitConverter.GetBytes((short)input);
                return true;
            }

            if (inputType == typeof(uint))
            {
                value = BitConverter.GetBytes((uint)input);
                return true;
            }

            if (inputType == typeof(ulong))
            {
                value = BitConverter.GetBytes((ulong)input);
                return true;
            }

            if (inputType == typeof(ushort))
            {
                value = BitConverter.GetBytes((ushort)input);
                return true;
            }

            if (inputType == typeof(bool))
            {
                value = BitConverter.GetBytes((bool)input);
                return true;
            }

            if (inputType == typeof(char))
            {
                value = BitConverter.GetBytes((char)input);
                return true;
            }

            if (inputType == typeof(float))
            {
                value = BitConverter.GetBytes((float)input);
                return true;
            }

            if (inputType == typeof(double))
            {
                value = BitConverter.GetBytes((double)input);
                return true;
            }

            if (inputType == typeof(byte))
            {
                value = new byte[] { (byte)input };
                return true;
            }

            if (inputType == typeof(sbyte))
            {
                value = new byte[] { unchecked((byte)(sbyte)input) };
                return true;
            }

            if (inputType == typeof(decimal))
            {
                value = ((decimal)value!).ToBytes();
                return true;
            }

            if (inputType == typeof(Guid))
            {
                value = ((Guid)input).ToByteArray();
                return true;
            }
        }

        var tc = TypeDescriptor.GetConverter(conversionType);
        if (tc != null && tc.CanConvertFrom(inputType))
        {
            try
            {
                value = tc.ConvertFrom(null, provider as CultureInfo, input);
                return true;
            }
            catch
            {
                // continue;
            }
        }

        tc = TypeDescriptor.GetConverter(inputType);
        if (tc != null && tc.CanConvertTo(conversionType))
        {
            try
            {
                value = tc.ConvertTo(null, provider as CultureInfo, input, conversionType);
                return true;
            }
            catch
            {
                // continue;
            }
        }

        if (input is IConvertible convertible)
        {
            try
            {
                value = convertible.ToType(conversionType, provider);
                return true;
            }
            catch
            {
                // continue
            }
        }

        if (conversionType == typeof(string))
        {
            value = string.Format(provider, "{0}", input);
            return true;
        }

        return false;
    }

    public static ulong EnumToUInt64(string? text, Type enumType)
    {
        if (enumType == null)
            throw new ArgumentNullException(nameof(enumType));

        return EnumToUInt64(ChangeType(text, enumType)!);
    }

    public static ulong EnumToUInt64(object value)
    {
        if (value == null)
            throw new ArgumentNullException(nameof(value));

        var typeCode = Convert.GetTypeCode(value);
        return typeCode switch
        {
            TypeCode.SByte or TypeCode.Int16 or TypeCode.Int32 or TypeCode.Int64 => (ulong)Convert.ToInt64(value, CultureInfo.CurrentCulture),
            TypeCode.Byte or TypeCode.UInt16 or TypeCode.UInt32 or TypeCode.UInt64 => Convert.ToUInt64(value, CultureInfo.CurrentCulture),
            _ => ChangeType<ulong>(value, 0, CultureInfo.CurrentCulture),
        };
    }

    private static bool StringToEnum(Type type, string[] names, Array values, string input, out object value)
    {
        for (var i = 0; i < names.Length; i++)
        {
            if (names[i].EqualsIgnoreCase(input))
            {
                value = values.GetValue(i)!;
                return true;
            }
        }

        for (var i = 0; i < values.GetLength(0); i++)
        {
            var valuei = values.GetValue(i)!;
            if (input.Length > 0 && input[0] == '-')
            {
                var ul = (long)EnumToUInt64(valuei);
                if (ul.ToString(CultureInfo.CurrentCulture).EqualsIgnoreCase(input))
                {
                    value = valuei;
                    return true;
                }
            }
            else
            {
                var ul = EnumToUInt64(valuei);
                if (ul.ToString(CultureInfo.CurrentCulture).EqualsIgnoreCase(input))
                {
                    value = valuei;
                    return true;
                }
            }
        }

        if (char.IsDigit(input[0]) || input[0] == '-' || input[0] == '+')
        {
            var obj = EnumToObject(type, input);
            if (obj == null)
            {
                value = Activator.CreateInstance(type)!;
                return false;
            }
            value = obj;
            return true;
        }

        value = Activator.CreateInstance(type)!;
        return false;
    }

    public static object EnumToObject(Type enumType, object value)
    {
        if (enumType == null)
            throw new ArgumentNullException(nameof(enumType));

        if (!enumType.IsEnum)
            throw new ArgumentException(null, nameof(enumType));

        if (value == null)
            throw new ArgumentNullException(nameof(value));

        var underlyingType = Enum.GetUnderlyingType(enumType);
        if (underlyingType == typeof(long))
            return Enum.ToObject(enumType, ChangeType<long>(value));

        if (underlyingType == typeof(ulong))
            return Enum.ToObject(enumType, ChangeType<ulong>(value));

        if (underlyingType == typeof(int))
            return Enum.ToObject(enumType, ChangeType<int>(value));

        if ((underlyingType == typeof(uint)))
            return Enum.ToObject(enumType, ChangeType<uint>(value));

        if (underlyingType == typeof(short))
            return Enum.ToObject(enumType, ChangeType<short>(value));

        if (underlyingType == typeof(ushort))
            return Enum.ToObject(enumType, ChangeType<ushort>(value));

        if (underlyingType == typeof(byte))
            return Enum.ToObject(enumType, ChangeType<byte>(value));

        if (underlyingType == typeof(sbyte))
            return Enum.ToObject(enumType, ChangeType<sbyte>(value));

        throw new ArgumentException(null, nameof(enumType));
    }

    public static object ToEnum(object obj, Enum defaultValue)
    {
        if (defaultValue == null)
            throw new ArgumentNullException(nameof(defaultValue));

        if (obj == null)
            return defaultValue;

        if (obj.GetType() == defaultValue.GetType())
            return obj;

        if (EnumTryParse(defaultValue.GetType(), obj.ToString(), out object value))
            return value;

        return defaultValue;
    }

    public static object ToEnum(string? text, Type enumType)
    {
        if (enumType == null)
            throw new ArgumentNullException(nameof(enumType));

        EnumTryParse(enumType, text, out object value);
        return value;
    }

    public static Enum ToEnum(string? text, Enum defaultValue)
    {
        if (defaultValue == null)
            throw new ArgumentNullException(nameof(defaultValue));

        if (EnumTryParse(defaultValue.GetType(), text, out object value))
            return (Enum)value;

        return defaultValue;
    }

    public static bool EnumTryParse(Type type, object? input, out object value)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        if (!type.IsEnum)
            throw new ArgumentException(null, nameof(type));

        if (input == null)
        {
            value = Activator.CreateInstance(type)!;
            return false;
        }

        var stringInput = string.Format(CultureInfo.CurrentCulture, "{0}", input);
        stringInput = stringInput.Nullify();
        if (stringInput == null)
        {
            value = Activator.CreateInstance(type)!;
            return false;
        }

        if (stringInput.StartsWith("0x", StringComparison.OrdinalIgnoreCase) && ulong.TryParse(stringInput.Substring(2), NumberStyles.HexNumber, CultureInfo.CurrentCulture, out ulong ulx))
        {
            value = ToEnum(ulx.ToString(CultureInfo.CurrentCulture), type);
            return true;
        }

        var names = Enum.GetNames(type);
        if (names.Length == 0)
        {
            value = Activator.CreateInstance(type)!;
            return false;
        }

        var values = Enum.GetValues(type);
        // some enums like System.CodeDom.MemberAttributes *are* flags but are not declared with Flags...
        if (!type.IsDefined(typeof(FlagsAttribute), true) && stringInput.IndexOfAny(_enumSeparators) < 0)
            return StringToEnum(type, names, values, stringInput, out value);

        // multi value enum
        var tokens = stringInput.Split(_enumSeparators, StringSplitOptions.RemoveEmptyEntries);
        if (tokens.Length == 0)
        {
            value = Activator.CreateInstance(type)!;
            return false;
        }

        ulong ul = 0;
        foreach (var tok in tokens)
        {
            var token = tok.Nullify(); // NOTE: we don't consider empty tokens as errors
            if (token == null)
                continue;

            if (!StringToEnum(type, names, values, token, out object tokenValue))
            {
                value = Activator.CreateInstance(type)!;
                return false;
            }

            var tokenUl = Convert.GetTypeCode(tokenValue) switch
            {
                TypeCode.Int16 or TypeCode.Int32 or TypeCode.Int64 or TypeCode.SByte => (ulong)Convert.ToInt64(tokenValue, CultureInfo.CurrentCulture),
                _ => Convert.ToUInt64(tokenValue, CultureInfo.CurrentCulture),
            };
            ul |= tokenUl;
        }
        value = Enum.ToObject(type, ul);
        return true;
    }

    public static string? GetNullifiedValue(this IDictionary<string, string?> dictionary, string key, string? defaultValue = null)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));

        if (dictionary == null)
            return defaultValue;

        if (!dictionary.TryGetValue(key, out var str))
            return defaultValue;

        return str.Nullify();
    }

    public static T? GetValue<T>(this IDictionary<string, object?> dictionary, string key, T? defaultValue)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));

        if (dictionary == null)
            return defaultValue;

        if (!dictionary.TryGetValue(key, out var o))
            return defaultValue;

        return ChangeType(o, defaultValue);
    }

    public static T? GetValue<T>(this IDictionary<string, object?> dictionary, string key, T? defaultValue, IFormatProvider? provider)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));

        if (dictionary == null)
            return defaultValue;

        if (!dictionary.TryGetValue(key, out var o))
            return defaultValue;

        return ChangeType(o, defaultValue, provider);
    }

    public static T? GetValue<T>(this IDictionary<string, string?> dictionary, string key, T? defaultValue)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));

        if (dictionary == null)
            return defaultValue;

        if (!dictionary.TryGetValue(key, out var str))
            return defaultValue;

        return ChangeType(str, defaultValue);
    }

    public static T? GetValue<T>(this IDictionary<string, string?> dictionary, string key, T? defaultValue, IFormatProvider? provider)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));

        if (dictionary == null)
            return defaultValue;

        if (!dictionary.TryGetValue(key, out var str))
            return defaultValue;

        return ChangeType(str, defaultValue, provider);
    }

    public static bool Compare<TKey, TValue>(this IDictionary<TKey, TValue> dic1, IDictionary<TKey, TValue> dic2, IEqualityComparer<TValue>? comparer = null)
    {
        if (dic1 == null)
            return dic2 == null;

        if (dic2 == null)
            return false;

        if (dic1.Count != dic2.Count)
            return false;

        comparer ??= EqualityComparer<TValue>.Default;

        foreach (var kv1 in dic1)
        {
            if (!dic2.TryGetValue(kv1.Key, out var s2) || !comparer.Equals(s2, kv1.Value))
                return false;
        }

        foreach (var kv2 in dic2)
        {
            if (!dic1.TryGetValue(kv2.Key, out var s1) || !comparer.Equals(s1, kv2.Value))
                return false;
        }
        return true;
    }
}
