namespace SqlNado;

public class SQLiteQueryTranslator(SQLiteDatabase database, TextWriter writer) : ExpressionVisitor
{
    private SQLiteBindOptions? _bindOptions;

    public SQLiteDatabase Database { get; } = database ?? throw new ArgumentNullException(nameof(database));
    public TextWriter Writer { get; } = writer ?? throw new ArgumentNullException(nameof(writer));
    public SQLiteBindOptions? BindOptions { get => _bindOptions ?? Database.BindOptions; set => _bindOptions = value; }
    public int? Skip { get; private set; }
    public int? Take { get; private set; }

    private static string BuildNotSupported(string text) => "0023: " + text + " is not handled by the Expression Translator.";

    public virtual void Translate(Expression expression)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));

        var expr = PartialEvaluator.Eval(expression);
        Visit(expr);
        if (Skip.HasValue || Take.HasValue)
        {
            Writer.Write(" LIMIT ");
            if (Take.HasValue)
            {
                Writer.Write(Take.Value);
            }
            else
            {
                Writer.Write("-1");
            }

            if (Skip.HasValue)
            {
                Writer.Write(" OFFSET ");
                Writer.Write(Skip.Value);
            }
        }
    }

    protected virtual string SubTranslate(Expression expression)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));

        using var writer = new StringWriter();
        var translator = new SQLiteQueryTranslator(Database, writer)
        {
            BindOptions = BindOptions
        };
        translator.Visit(expression);
        return writer.ToString();
    }

    private static Expression StripQuotes(Expression expression)
    {
        while (expression.NodeType == ExpressionType.Quote)
        {
            expression = ((UnaryExpression)expression).Operand;
        }
        return expression;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.DeclaringType == typeof(Queryable))
        {
            switch (node.Method.Name)
            {
                case nameof(Queryable.Where):
                    Writer.Write("SELECT * FROM (");
                    Visit(node.Arguments[0]);
                    Writer.Write(") AS T WHERE ");
                    var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                    Visit(lambda.Body);
                    return node;

                case nameof(Queryable.OrderBy):
                case nameof(Queryable.OrderByDescending):
                    Visit(node.Arguments[0]);
                    Writer.Write(" ORDER BY ");
                    Visit(node.Arguments[1]);
                    if (string.Equals(node.Method.Name, nameof(Queryable.OrderByDescending), StringComparison.Ordinal))
                    {
                        Writer.Write(" DESC");
                    }
                    return node;

                case nameof(Queryable.ThenBy):
                case nameof(Queryable.ThenByDescending):
                    Visit(node.Arguments[0]);
                    Writer.Write(", ");
                    Visit(node.Arguments[1]);
                    if (string.Equals(node.Method.Name, nameof(Queryable.ThenByDescending), StringComparison.Ordinal))
                    {
                        Writer.Write(" DESC");
                    }
                    return node;

                case nameof(Queryable.First):
                case nameof(Queryable.FirstOrDefault):
                    Visit(node.Arguments[0]);
                    Take = 1;
                    return node;

                case nameof(Queryable.Take):
                    Visit(node.Arguments[0]);
                    Take = (int)((ConstantExpression)node.Arguments[1]).Value!;
                    return node;

                case nameof(Queryable.Skip):
                    Visit(node.Arguments[0]);
                    Skip = (int)((ConstantExpression)node.Arguments[1]).Value!;
                    return node;
            }
        }

        if (node.Method.DeclaringType == typeof(Conversions))
        {
            switch (node.Method.Name)
            {
                case nameof(Conversions.EqualsIgnoreCase):
                    Visit(node.Arguments[0]);
                    Writer.Write(" = ");
                    Visit(node.Arguments[1]);
                    Writer.Write(" COLLATE " + nameof(StringComparer.OrdinalIgnoreCase));
                    return node;
            }
        }

        if (node.Method.DeclaringType == typeof(string))
        {
            switch (node.Method.Name)
            {
                case nameof(string.StartsWith):
                case nameof(string.EndsWith):
                case nameof(string.Contains):
                    Visit(node.Object);
                    Writer.Write(" LIKE ");

                    var sub = SubTranslate(node.Arguments[0]);
                    if (IsQuoted(sub))
                    {
                        Writer.Write('\'');
                        if (string.Equals(node.Method.Name, nameof(string.EndsWith), StringComparison.Ordinal) ||
                            string.Equals(node.Method.Name, nameof(string.Contains), StringComparison.Ordinal))
                        {
                            Writer.Write('%');
                        }
                        Writer.Write(sub.Substring(1, sub.Length - 2));
                        if (string.Equals(node.Method.Name, nameof(string.StartsWith), StringComparison.Ordinal) ||
                            string.Equals(node.Method.Name, nameof(string.Contains), StringComparison.Ordinal))
                        {
                            Writer.Write('%');
                        }
                        Writer.Write('\'');
                    }
                    else
                    {
                        Writer.Write(sub);
                    }

                    if (node.Arguments.Count > 1 &&
                        node.Arguments[1] is ConstantExpression ce1 &&
                        ce1.Value is StringComparison sc1)
                    {
                        Writer.Write(" COLLATE ");
                        Writer.Write(sc1.ToString());
                    }
                    return node;

                case nameof(string.ToLower):
                    Writer.Write("lower(");
                    Visit(node.Object);
                    Writer.Write(')');
                    return node;

                case nameof(string.ToUpper):
                    Writer.Write("upper(");
                    Visit(node.Object);
                    Writer.Write(')');
                    return node;

                case nameof(string.IndexOf):
                    if (node.Arguments.Count > 1 &&
                        node.Arguments[1] is ConstantExpression ce2 &&
                        ce2.Value is StringComparison sc2)
                    {
                        Database.EnsureQuerySupportFunctions();
                        Writer.Write("(instr(");
                        Visit(node.Object);
                        Writer.Write(',');
                        Visit(node.Arguments[0]);
                        Writer.Write(',');
                        Writer.Write((int)sc2);
                        Writer.Write(")");
                        Writer.Write("-1)"); // SQLite is 1-based
                    }
                    else
                    {
                        Writer.Write("(instr(");
                        Visit(node.Object);
                        Writer.Write(',');
                        Visit(node.Arguments[0]);
                        Writer.Write(")");
                        Writer.Write("-1)"); // SQLite is 1-based
                    }
                    return node;

                case nameof(string.Substring):
                    Writer.Write("substr(");
                    Visit(node.Object);
                    Writer.Write(",(");
                    Visit(node.Arguments[0]);
                    Writer.Write("+1)"); // SQLite is 1-based
                    if (node.Arguments.Count > 1)
                    {
                        Writer.Write(',');
                        Visit(node.Arguments[1]);
                    }
                    Writer.Write(')');
                    return node;
            }
        }

        if (node.Method.DeclaringType == typeof(Enum))
        {
            switch (node.Method.Name)
            {
                case nameof(Enum.HasFlag):
                    Visit(node.Object);
                    Writer.Write(" & ");
                    Visit(node.Arguments[0]);
                    return node;
            }
        }

        if (node.Method.DeclaringType == typeof(Convert))
        {
            switch (node.Method.Name)
            {
                case nameof(Convert.IsDBNull):
                    Visit(node.Arguments[0]);
                    Writer.Write(" IS NULL");
                    return node;
            }
        }

        if (node.Method.DeclaringType == typeof(object))
        {
            switch (node.Method.Name)
            {
                case nameof(object.Equals):
                    Visit(node.Object);
                    Writer.Write(" = ");
                    Visit(node.Arguments[0]);
                    return node;
            }
        }

        if (node.Method.DeclaringType == typeof(Math))
        {
            switch (node.Method.Name)
            {
                case nameof(Math.Abs):
                    Writer.Write("abs(");
                    Visit(node.Arguments[0]);
                    Writer.Write(')');
                    return node;
            }
        }

        if (node.Method.DeclaringType == typeof(QueryExtensions))
        {
            switch (node.Method.Name)
            {
                case nameof(QueryExtensions.Contains):
                    if (node.Arguments.Count > 2 &&
                        node.Arguments[2] is ConstantExpression ce3 &&
                        ce3.Value is StringComparison sc3)
                    {
                        Database.EnsureQuerySupportFunctions();
                        Writer.Write("(instr(");
                        Visit(node.Arguments[0]);
                        Writer.Write(',');
                        Visit(node.Arguments[1]);
                        Writer.Write(',');
                        Writer.Write((int)sc3);
                        Writer.Write(")");
                        Writer.Write(">0)"); // SQLite is 1-based
                    }
                    return node;
            }
        }

        // kinda hack: generic ToString handling
        if (string.Equals(node.Method.Name, nameof(ToString), StringComparison.Ordinal) &&
            node.Method.GetParameters().Length == 0)
        {
            Visit(node.Object);
            return node;
        }

        throw new SqlNadoException(BuildNotSupported("The method '" + node.Method.Name + "' of type '" + node?.Method?.DeclaringType?.FullName + "'"));
    }

    private static bool IsQuoted(string s) => s != null && s.Length > 1 && s.StartsWith("'", StringComparison.Ordinal) && s.EndsWith("'", StringComparison.Ordinal);

    protected override Expression VisitUnary(UnaryExpression node)
    {
        switch (node.NodeType)
        {
            case ExpressionType.Not:
                Writer.Write(" NOT (");
                Visit(node.Operand);
                Writer.Write(")");
                break;

            case ExpressionType.ArrayLength:
                Writer.Write(" length(");
                Visit(node.Operand);
                Writer.Write(")");
                break;

            case ExpressionType.Quote:
                Visit(node.Operand);
                break;

            // just let go. hopefully it should be ok with sqlite
            case ExpressionType.Convert:
                Visit(node.Operand);
                break;

            default:
                throw new SqlNadoException(BuildNotSupported("The unary operator '" + node.NodeType + "'"));

        }
        return node;
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        Writer.Write("(");
        Visit(node.Left);
        switch (node.NodeType)
        {
            case ExpressionType.Add:
            case ExpressionType.AddChecked:
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
            case ExpressionType.MultiplyChecked:
                Writer.Write(" * ");
                break;

            case ExpressionType.Negate:
            case ExpressionType.NegateChecked:
                Writer.Write(" ! ");
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
            case ExpressionType.SubtractChecked:
                Writer.Write(" - ");
                break;

            default:
                throw new SqlNadoException(BuildNotSupported("The binary operator '" + node.NodeType + "'"));
        }

        Visit(node.Right);
        Writer.Write(")");
        return node;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        if (node.Value is IQueryable queryable)
        {
            var table = Database.GetObjectTable(queryable.ElementType);
            Writer.Write(table.EscapedName);
        }
        else if (node.Value == null)
        {
            Writer.Write("NULL");
        }
        else
        {
            var value = Database.CoerceValueForBind(node.Value, BindOptions);
            if (value == null)
            {
                Writer.Write("NULL");
            }
            else
            {
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
                        s = s.Replace("'", "''");
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
                            var hex = "X'" + Conversions.ToHexa(bytes) + "'";
                            Writer.Write(hex);
                            break;
                        }

                        throw new SqlNadoException(BuildNotSupported("The constant '" + value + " of type '" + value.GetType().FullName + "' (from expression value constant '" + node.Value + "' of type '" + node.Value.GetType().FullName + "') for '" + value + "'"));
                }
            }
        }
        return node;
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        if (node.Expression != null)
        {
            if (node.Expression.NodeType == ExpressionType.Parameter)
            {
                var table = Database.GetObjectTable(node.Expression.Type);
                var col = table.GetColumn(node.Member.Name);
                if (col != null)
                {
                    // we don't use double-quoted escaped column name here
                    Writer.Write('[');
                    Writer.Write(col.Name);
                    Writer.Write(']');
                }
                else
                {
                    Writer.Write(node.Member.Name);
                }
                return node;
            }

            if (node.Member != null && node.Member.DeclaringType == typeof(string) && string.Equals(node.Member.Name, nameof(string.Length), StringComparison.Ordinal))
            {
                Writer.Write(" length(");
                Visit(node.Expression);
                Writer.Write(')');
                return node;
            }
        }

        throw new SqlNadoException(BuildNotSupported("The member '" + node.Member?.Name + "'"));
    }

    // from https://github.com/mattwar/iqtoolkit
    private sealed class PartialEvaluator
    {
        public static Expression? Eval(Expression expression) => Eval(expression, null, null);
        public static Expression? Eval(Expression expression, Func<Expression, bool>? fnCanBeEvaluated) => Eval(expression, fnCanBeEvaluated, null);
        public static Expression? Eval(Expression expression, Func<Expression, bool>? fnCanBeEvaluated, Func<ConstantExpression, Expression>? fnPostEval)
        {
            fnCanBeEvaluated ??= CanBeEvaluatedLocally;
            return SubtreeEvaluator.DoEval(Nominator.Nominate(fnCanBeEvaluated, expression), fnPostEval, expression);
        }

        private static bool CanBeEvaluatedLocally(Expression expression) => expression.NodeType != ExpressionType.Parameter;

        private sealed class SubtreeEvaluator : ExpressionVisitor
        {
            private readonly HashSet<Expression> _candidates;
            private readonly Func<ConstantExpression, Expression>? _evalFunc;

            private SubtreeEvaluator(HashSet<Expression> candidates, Func<ConstantExpression, Expression>? evalFunc)
            {
                _candidates = candidates;
                _evalFunc = evalFunc;
            }

            internal static Expression? DoEval(HashSet<Expression> candidates, Func<ConstantExpression, Expression>? onEval, Expression exp) => new SubtreeEvaluator(candidates, onEval).Visit(exp);

            public override Expression? Visit(Expression? node)
            {
                if (node == null)
                    return null;

                if (_candidates.Contains(node))
                    return Evaluate(node);

                return base.Visit(node);
            }

            private Expression PostEval(ConstantExpression constant)
            {
                if (_evalFunc != null)
                    return _evalFunc(constant);

                return constant;
            }

            private Expression Evaluate(Expression expression)
            {
                var modified = false;
                var type = expression.Type;
                if (expression.NodeType == ExpressionType.Convert)
                {
                    var u = (UnaryExpression)expression;
                    if (GetNonNullableType(u.Operand.Type) == GetNonNullableType(type))
                    {
                        expression = ((UnaryExpression)expression).Operand;
                        modified = true;
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
                    modified = true;
                }

                // avoid stack overflow (infinite recursion)
                if (!modified && expression is MethodCallExpression mce)
                {
                    var parameters = mce.Method.GetParameters();
                    if (parameters.Length == 0 && mce.Method.ReturnType == type)
                        return expression;

                    // like Queryable extensions methods (First, FirstOrDefault, etc.)
                    if (parameters.Length == 1 && mce.Method.IsStatic)
                    {
                        var iqt = typeof(IQueryable<>).MakeGenericType(type);
                        if (iqt == parameters[0].ParameterType)
                            return expression;
                    }
                }

                var lambda = Expression.Lambda<Func<object>>(expression);
                Func<object> fn = lambda.Compile();
                var constant = Expression.Constant(fn(), type);
                return PostEval(constant);
            }

            private static object? GetValue(MemberInfo member, object? instance) => member.MemberType switch
            {
                MemberTypes.Property => ((PropertyInfo)member).GetValue(instance, null),
                MemberTypes.Field => ((FieldInfo)member).GetValue(instance),
                _ => throw new InvalidOperationException(),
            };

            private static bool IsNullableType(Type type) => type != null && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
            private static Type GetNonNullableType(Type type) => IsNullableType(type) ? type.GetGenericArguments()[0] : type;
        }

        private sealed class Nominator(Func<Expression, bool> fnCanBeEvaluated) : ExpressionVisitor
        {
            private readonly Func<Expression, bool> _fnCanBeEvaluated = fnCanBeEvaluated;
            private readonly HashSet<Expression> _candidates = [];
            private bool _cannotBeEvaluated;

            public static HashSet<Expression> Nominate(Func<Expression, bool> fnCanBeEvaluated, Expression expression)
            {
                var nominator = new Nominator(fnCanBeEvaluated);
                nominator.Visit(expression);
                return nominator._candidates;
            }

            public override Expression? Visit(Expression? node)
            {
                if (node != null)
                {
                    var saveCannotBeEvaluated = _cannotBeEvaluated;
                    _cannotBeEvaluated = false;
                    base.Visit(node);
                    if (!_cannotBeEvaluated)
                    {
                        if (_fnCanBeEvaluated(node))
                        {
                            _candidates.Add(node);
                        }
                        else
                        {
                            _cannotBeEvaluated = true;
                        }
                    }

                    _cannotBeEvaluated |= saveCannotBeEvaluated;
                }
                return node;
            }
        }
    }
}
