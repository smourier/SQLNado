using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteQueryTranslator : ExpressionVisitor
    {
        private SQLiteTypeOptions _typeOptions;
        public SQLiteQueryTranslator(SQLiteDatabase database, TextWriter writer)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (writer == null)
                throw new ArgumentNullException(nameof(writer));

            Database = database;
            Writer = writer;
        }

        public SQLiteDatabase Database { get; }
        public TextWriter Writer { get; }
        public SQLiteTypeOptions TypeOptions { get => _typeOptions ?? Database.TypeOptions; set => _typeOptions = value; }

        private static string BuildNotSupported(string text) => "0023: " + text + " is not handled by the Expression Translator.";

        public virtual void Translate(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            expression = PartialEvaluator.Eval(expression);
            Visit(expression);
        }

        protected virtual string SubTranslate(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            using (var writer = new StringWriter())
            {
                var translator = new SQLiteQueryTranslator(Database, writer);
                translator.TypeOptions = TypeOptions;
                translator.Visit(expression);
                return writer.ToString();
            }
        }

        private static Expression StripQuotes(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Quote)
            {
                expression = ((UnaryExpression)expression).Operand;
            }
            return expression;
        }

        protected override Expression VisitMethodCall(MethodCallExpression callExpression)
        {
            if (callExpression.Method.DeclaringType == typeof(Queryable))
            {
                switch (callExpression.Method.Name)
                {
                    case nameof(Queryable.Where):
                        Writer.Write("SELECT * FROM (");
                        Visit(callExpression.Arguments[0]);
                        Writer.Write(") AS T WHERE ");
                        var lambda = (LambdaExpression)StripQuotes(callExpression.Arguments[1]);
                        Visit(lambda.Body);
                        return callExpression;
                }
            }

            if (callExpression.Method.DeclaringType == typeof(Conversions))
            {
                switch (callExpression.Method.Name)
                {
                    case nameof(Conversions.EqualsIgnoreCase):
                        Visit(callExpression.Arguments[0]);
                        Writer.Write(" = ");
                        Visit(callExpression.Arguments[1]);
                        Writer.Write(" COLLATE " + nameof(StringComparer.OrdinalIgnoreCase));
                        return callExpression;
                }
            }

            if (callExpression.Method.DeclaringType == typeof(string))
            {
                switch (callExpression.Method.Name)
                {
                    case nameof(string.StartsWith):
                    case nameof(string.EndsWith):
                        Visit(callExpression.Object);
                        Writer.Write(" LIKE ");

                        string sub = SubTranslate(callExpression.Arguments[0]);
                        if (sub != null && sub.Length > 1 && sub.StartsWith("'") && sub.EndsWith("'"))
                        {
                            Writer.Write('\'');
                            if (callExpression.Method.Name == nameof(string.EndsWith))
                            {
                                Writer.Write('%');
                            }
                            Writer.Write(sub.Substring(1, sub.Length - 2));
                            if (callExpression.Method.Name == nameof(string.StartsWith))
                            {
                                Writer.Write('%');
                            }
                            Writer.Write('\'');
                        }
                        else
                        {
                            Writer.Write(sub);
                        }
                        return callExpression;
                }
            }

            if (callExpression.Method.DeclaringType == typeof(Enum))
            {
                switch (callExpression.Method.Name)
                {
                    case nameof(Enum.HasFlag):
                        Visit(callExpression.Object);
                        Writer.Write(" & ");
                        Visit(callExpression.Arguments[0]);
                        return callExpression;
                }
            }

            throw new SqlNadoException(BuildNotSupported("The method '" + callExpression.Method.Name + "' of type '" + callExpression.Method.DeclaringType.FullName + "'"));
        }

        protected override Expression VisitUnary(UnaryExpression unaryExpression)
        {
            switch (unaryExpression.NodeType)
            {
                case ExpressionType.Not:
                    Writer.Write(" NOT (");
                    Visit(unaryExpression.Operand);
                    Writer.Write(")");
                    break;

                // just let go. hopefully it should be ok with sqlite
                case ExpressionType.Convert:
                    Visit(unaryExpression.Operand);
                    break;

                default:
                    throw new SqlNadoException(BuildNotSupported("The unary operator '" + unaryExpression.NodeType + "'"));

            }
            return unaryExpression;
        }

        protected override Expression VisitBinary(BinaryExpression binaryExpression)
        {
            Writer.Write("(");
            Visit(binaryExpression.Left);
            switch (binaryExpression.NodeType)
            {
                case ExpressionType.Add:
                    Writer.Write(" + ");
                    break;

                case ExpressionType.And:
                    Writer.Write(" & ");
                    break;

                case ExpressionType.AndAlso:
                    Writer.Write(" AND ");
                    break;

                case ExpressionType.Divide:
                    Writer.Write(" / ");
                    break;

                case ExpressionType.Equal:
                    Writer.Write(" = ");
                    break;

                case ExpressionType.GreaterThan:
                    Writer.Write(" > ");
                    break;

                case ExpressionType.GreaterThanOrEqual:
                    Writer.Write(" >= ");
                    break;

                case ExpressionType.LeftShift:
                    Writer.Write(" << ");
                    break;

                case ExpressionType.LessThan:
                    Writer.Write(" < ");
                    break;

                case ExpressionType.LessThanOrEqual:
                    Writer.Write(" <= ");
                    break;

                case ExpressionType.Modulo:
                    Writer.Write(" % ");
                    break;

                case ExpressionType.Multiply:
                    Writer.Write(" * ");
                    break;

                case ExpressionType.NotEqual:
                    Writer.Write(" <> ");
                    break;

                case ExpressionType.OnesComplement:
                    Writer.Write(" ~ ");
                    break;

                case ExpressionType.Or:
                    Writer.Write(" | ");
                    break;

                case ExpressionType.OrElse:
                    Writer.Write(" OR ");
                    break;

                case ExpressionType.RightShift:
                    Writer.Write(" >> ");
                    break;

                case ExpressionType.Subtract:
                    Writer.Write(" - ");
                    break;

                default:
                    throw new SqlNadoException(BuildNotSupported("The binary operator '" + binaryExpression.NodeType + "'"));
            }

            Visit(binaryExpression.Right);
            Writer.Write(")");
            return binaryExpression;
        }

        protected override Expression VisitConstant(ConstantExpression constant)
        {
            if (constant.Value is IQueryable queryable)
            {
                var table = Database.GetObjectTable(queryable.ElementType);
                Writer.Write(table.EscapedName);
            }
            else if (constant.Value == null)
            {
                Writer.Write("NULL");
            }
            else
            {
                object value = Database.CoerceValueForBind(constant.Value, TypeOptions);
                switch (Type.GetTypeCode(value.GetType()))
                {
                    case TypeCode.Boolean:
                        Writer.Write(((bool)value) ? 1 : 0);
                        break;

                    case TypeCode.DBNull:
                        Writer.Write("NULL");
                        break;

                    case TypeCode.Double:
                        break;

                    case TypeCode.String:
                        var s = (string)value;
                        if (s != null)
                        {
                            s = s.Replace("'", "''");
                        }

                        Writer.Write('\'');
                        Writer.Write(s);
                        Writer.Write('\'');
                        break;

                    case TypeCode.Int32:
                    case TypeCode.Int64:
                        Writer.Write(string.Format(CultureInfo.InvariantCulture, "{0}", value));
                        break;

                    default:
                        if (value is byte[] bytes)
                        {
                            break;
                        }

                        throw new SqlNadoException(BuildNotSupported("The constant '" + value + " of type '" + value.GetType().FullName + "' (from expression value constant '" + constant.Value + "' of type '" + constant.Value.GetType().FullName + "') for '" + value + "'"));
                }
            }
            return constant;
        }

        protected override Expression VisitMember(MemberExpression memberExpression)
        {
            if (memberExpression.Expression != null)
            {
                if (memberExpression.Expression.NodeType == ExpressionType.Parameter)
                {
                    Writer.Write(memberExpression.Member.Name);
                    return memberExpression;
                }
            }

            throw new SqlNadoException(BuildNotSupported("The member '" + memberExpression.Member.Name + "'"));
        }

        // from https://github.com/mattwar/iqtoolkit
        private class PartialEvaluator
        {
            public static Expression Eval(Expression expression) => Eval(expression, null, null);
            public static Expression Eval(Expression expression, Func<Expression, bool> fnCanBeEvaluated) => Eval(expression, fnCanBeEvaluated, null);
            public static Expression Eval(Expression expression, Func<Expression, bool> fnCanBeEvaluated, Func<ConstantExpression, Expression> fnPostEval)
            {
                if (fnCanBeEvaluated == null)
                {
                    fnCanBeEvaluated = CanBeEvaluatedLocally;
                }
                return SubtreeEvaluator.Eval(Nominator.Nominate(fnCanBeEvaluated, expression), fnPostEval, expression);
            }

            private static bool CanBeEvaluatedLocally(Expression expression) => expression.NodeType != ExpressionType.Parameter;

            private class SubtreeEvaluator : ExpressionVisitor
            {
                private HashSet<Expression> _candidates;
                private Func<ConstantExpression, Expression> _evalFunc;

                private SubtreeEvaluator(HashSet<Expression> candidates, Func<ConstantExpression, Expression> evalFunc)
                {
                    _candidates = candidates;
                    _evalFunc = evalFunc;
                }

                internal static Expression Eval(HashSet<Expression> candidates, Func<ConstantExpression, Expression> onEval, Expression exp) => new SubtreeEvaluator(candidates, onEval).Visit(exp);

                public override Expression Visit(Expression expression)
                {
                    if (expression == null)
                        return null;

                    if (_candidates.Contains(expression))
                        return Evaluate(expression);

                    return base.Visit(expression);
                }

                private Expression PostEval(ConstantExpression constant)
                {
                    if (_evalFunc != null)
                        return _evalFunc(constant);

                    return constant;
                }

                private Expression Evaluate(Expression expression)
                {
                    var type = expression.Type;
                    if (expression.NodeType == ExpressionType.Convert)
                    {
                        var u = (UnaryExpression)expression;
                        if (GetNonNullableType(u.Operand.Type) == GetNonNullableType(type))
                        {
                            expression = ((UnaryExpression)expression).Operand;
                        }
                    }

                    if (expression.NodeType == ExpressionType.Constant)
                    {
                        if (expression.Type == type)
                            return expression;

                        if (GetNonNullableType(expression.Type) == GetNonNullableType(type))
                            return Expression.Constant(((ConstantExpression)expression).Value, type);
                    }

                    if (expression is MemberExpression me && me.Expression is ConstantExpression ce)
                        return PostEval(Expression.Constant(GetValue(me.Member, ce.Value), type));

                    if (type.IsValueType)
                    {
                        expression = Expression.Convert(expression, typeof(object));
                    }

                    var lambda = Expression.Lambda<Func<object>>(expression);
                    Func<object> fn = lambda.Compile();
                    return PostEval(Expression.Constant(fn(), type));
                }

                private static object GetValue(MemberInfo member, object instance)
                {
                    switch (member.MemberType)
                    {
                        case MemberTypes.Property:
                            return ((PropertyInfo)member).GetValue(instance, null);

                        case MemberTypes.Field:
                            return ((FieldInfo)member).GetValue(instance);

                        default:
                            throw new InvalidOperationException();
                    }
                }

                private static bool IsNullableType(Type type) => type != null && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
                private static Type GetNonNullableType(Type type) => IsNullableType(type) ? type.GetGenericArguments()[0] : type;
            }

            private class Nominator : ExpressionVisitor
            {
                private Func<Expression, bool> _fnCanBeEvaluated;
                private HashSet<Expression> _candidates;
                private bool _cannotBeEvaluated;

                public Nominator(Func<Expression, bool> fnCanBeEvaluated)
                {
                    _candidates = new HashSet<Expression>();
                    _fnCanBeEvaluated = fnCanBeEvaluated;
                }

                public static HashSet<Expression> Nominate(Func<Expression, bool> fnCanBeEvaluated, Expression expression)
                {
                    var nominator = new Nominator(fnCanBeEvaluated);
                    nominator.Visit(expression);
                    return nominator._candidates;
                }

                protected override Expression VisitConstant(ConstantExpression c) => base.VisitConstant(c);

                public override Expression Visit(Expression expression)
                {
                    if (expression != null)
                    {
                        bool saveCannotBeEvaluated = _cannotBeEvaluated;
                        _cannotBeEvaluated = false;
                        base.Visit(expression);
                        if (!_cannotBeEvaluated)
                        {
                            if (_fnCanBeEvaluated(expression))
                            {
                                _candidates.Add(expression);
                            }
                            else
                            {
                                _cannotBeEvaluated = true;
                            }
                        }

                        _cannotBeEvaluated |= saveCannotBeEvaluated;
                    }
                    return expression;
                }
            }
        }
    }
}
