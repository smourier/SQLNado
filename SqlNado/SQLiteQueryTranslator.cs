using System;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteQueryTranslator : ExpressionVisitor
    {
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

        public virtual void Translate(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            Visit(expression);
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

            throw new NotSupportedException(string.Format("The method '{0}' is not supported", callExpression.Method.Name));
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

                default:
                    throw new NotSupportedException(string.Format("The unary operator '{0}' is not supported", unaryExpression.NodeType));

            }
            return unaryExpression;
        }

        protected override Expression VisitBinary(BinaryExpression binaryExpression)
        {
            Writer.Write("(");
            Visit(binaryExpression.Left);
            switch (binaryExpression.NodeType)
            {
                case ExpressionType.And:
                    Writer.Write(" AND ");
                    break;

                case ExpressionType.Or:
                    Writer.Write(" OR");
                    break;

                case ExpressionType.Equal:
                    Writer.Write(" = ");
                    break;

                case ExpressionType.NotEqual:
                    Writer.Write(" <> ");
                    break;

                case ExpressionType.LessThan:
                    Writer.Write(" < ");
                    break;

                case ExpressionType.LessThanOrEqual:
                    Writer.Write(" <= ");
                    break;

                case ExpressionType.GreaterThan:
                    Writer.Write(" > ");
                    break;

                case ExpressionType.GreaterThanOrEqual:
                    Writer.Write(" >= ");
                    break;

                default:
                    throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", binaryExpression.NodeType));
            }

            Visit(binaryExpression.Right);
            Writer.Write(")");
            return binaryExpression;
        }

        protected override Expression VisitConstant(ConstantExpression constantExpression)
        {
            if (constantExpression.Value is IQueryable queryable)
            {
                // assume constant nodes w/ IQueryables are table references
                var table = Database.GetObjectTable(queryable.ElementType);
                Writer.Write(table.EscapedName);
            }
            else if (constantExpression.Value == null || Convert.IsDBNull(constantExpression.Value))
            {
                Writer.Write("NULL");
            }
            else
            {
                switch (Type.GetTypeCode(constantExpression.Value.GetType()))
                {
                    case TypeCode.Boolean:
                        Writer.Write(((bool)constantExpression.Value) ? 1 : 0);
                        break;

                    case TypeCode.String:
                        Writer.Write("'");
                        Writer.Write(constantExpression.Value);
                        Writer.Write("'");
                        break;

                    case TypeCode.Object:
                        throw new NotSupportedException(string.Format("The constant for '{0}' is not supported", constantExpression.Value));

                    default:
                        Writer.Write(constantExpression.Value);
                        break;
                }
            }
            return constantExpression;
        }

        protected override Expression VisitMember(MemberExpression memberExpression)
        {
            if (memberExpression.Expression != null && memberExpression.Expression.NodeType == ExpressionType.Parameter)
            {
                Writer.Write(memberExpression.Member.Name);
                return memberExpression;
            }

            throw new NotSupportedException(string.Format("The member '{0}' is not supported", memberExpression.Member.Name));
        }
    }
}
