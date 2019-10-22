/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BindExpression;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.CurrentUser;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.function.Function;

import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class ExpressionFormatter
{
    private static final ThreadLocal<DecimalFormat> doubleFormatter = ThreadLocal.withInitial(
            () -> new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));

    private ExpressionFormatter() {}

    public static String formatExpression(Expression expression, Optional<List<Expression>> parameters)
    {
        return new Formatter(parameters).process(expression, null);
    }

    public static String formatQualifiedName(QualifiedName name)
    {
        return name.getParts().stream()
                .map(ExpressionFormatter::formatIdentifier)
                .collect(joining("."));
    }

    public static String formatIdentifier(String s)
    {
        return new StringBuilder().append('"').append(s.replace("\"", "\"\"")).append('"').toString();
    }

    static String formatStringLiteral(String s)
    {
        s = s.replace("'", "''");
        if (CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(s)) {
            return new StringBuilder().append("'").append(s).append("'").toString();
        }

        StringBuilder builder = new StringBuilder();
        builder.append("U&'");
        PrimitiveIterator.OfInt iterator = s.codePoints().iterator();
        while (iterator.hasNext()) {
            int codePoint = iterator.nextInt();
            checkArgument(codePoint >= 0, "Invalid UTF-8 encoding in characters: %s", s);
            if (isAsciiPrintable(codePoint)) {
                char ch = (char) codePoint;
                if (ch == '\\') {
                    builder.append(ch);
                }
                builder.append(ch);
            }
            else if (codePoint <= 0xFFFF) {
                builder.append('\\');
                builder.append(String.format("%04X", codePoint));
            }
            else {
                builder.append("\\+");
                builder.append(String.format("%06X", codePoint));
            }
        }
        builder.append("'");
        return builder.toString();
    }

	static String formatOrderBy(OrderBy orderBy, Optional<List<Expression>> parameters)
    {
        return "ORDER BY " + formatSortItems(orderBy.getSortItems(), parameters);
    }

	static String formatSortItems(List<SortItem> sortItems, Optional<List<Expression>> parameters)
    {
        return Joiner.on(", ").join(sortItems.stream()
                .map(sortItemFormatterFunction(parameters))
                .iterator());
    }

	static String formatGroupBy(List<GroupingElement> groupingElements)
    {
        return formatGroupBy(groupingElements, Optional.empty());
    }

	static String formatGroupBy(List<GroupingElement> groupingElements, Optional<List<Expression>> parameters)
    {
        ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

        groupingElements.forEach(groupingElement -> {
            String result = "";
            if (groupingElement instanceof SimpleGroupBy) {
                List<Expression> columns = ((SimpleGroupBy) groupingElement).getExpressions();
                if (columns.size() == 1) {
                    result = formatExpression(getOnlyElement(columns), parameters);
                }
                else {
                    result = formatGroupingSet(columns, parameters);
                }
            }
            else if (groupingElement instanceof GroupingSets) {
                result = format("GROUPING SETS (%s)", Joiner.on(", ").join(
                        ((GroupingSets) groupingElement).getSets().stream()
                                .map(e -> formatGroupingSet(e, parameters))
                                .iterator()));
            }
            else if (groupingElement instanceof Cube) {
                result = format("CUBE %s", formatGroupingSet(((Cube) groupingElement).getExpressions(), parameters));
            }
            else if (groupingElement instanceof Rollup) {
                result = format("ROLLUP %s", formatGroupingSet(((Rollup) groupingElement).getExpressions(), parameters));
            }
            resultStrings.add(result);
        });
        return Joiner.on(", ").join(resultStrings.build());
    }

	private static boolean isAsciiPrintable(int codePoint)
    {
        if (codePoint >= 0x7F || codePoint < 0x20) {
            return false;
        }
        return true;
    }

	private static String formatGroupingSet(List<Expression> groupingSet, Optional<List<Expression>> parameters)
    {
        return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
                .map(e -> formatExpression(e, parameters))
                .iterator()));
    }

	private static Function<SortItem, String> sortItemFormatterFunction(Optional<List<Expression>> parameters)
    {
        return input -> {
            StringBuilder builder = new StringBuilder();

            builder.append(formatExpression(input.getSortKey(), parameters));

            switch (input.getOrdering()) {
                case ASCENDING:
                    builder.append(" ASC");
                    break;
                case DESCENDING:
                    builder.append(" DESC");
                    break;
                default:
                    throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
            }

            switch (input.getNullOrdering()) {
                case FIRST:
                    builder.append(" NULLS FIRST");
                    break;
                case LAST:
                    builder.append(" NULLS LAST");
                    break;
                case UNDEFINED:
                    // no op
                    break;
                default:
                    throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
            }

            return builder.toString();
        };
    }

	public static class Formatter
            extends AstVisitor<String, Void>
    {
        private final Optional<List<Expression>> parameters;

        public Formatter(Optional<List<Expression>> parameters)
        {
            this.parameters = parameters;
        }

        @Override
        protected String visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitRow(Row node, Void context)
        {
            return new StringBuilder().append("ROW (").append(Joiner.on(", ").join(node.getItems().stream()
                    .map((child) -> process(child, context))
                    .collect(toList()))).append(")").toString();
        }

        @Override
        protected String visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitAtTimeZone(AtTimeZone node, Void context)
        {
            return new StringBuilder()
                    .append(process(node.getValue(), context))
                    .append(" AT TIME ZONE ")
                    .append(process(node.getTimeZone(), context)).toString();
        }

        @Override
        protected String visitCurrentUser(CurrentUser node, Void context)
        {
            return "CURRENT_USER";
        }

        @Override
        protected String visitCurrentTime(CurrentTime node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getFunction().getName());

            if (node.getPrecision() != null) {
                builder.append('(')
                        .append(node.getPrecision())
                        .append(')');
            }

            return builder.toString();
        }

        @Override
        protected String visitExtract(Extract node, Void context)
        {
            return new StringBuilder().append("EXTRACT(").append(node.getField()).append(" FROM ").append(process(node.getExpression(), context)).append(")").toString();
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context)
        {
            return formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitCharLiteral(CharLiteral node, Void context)
        {
            return "CHAR " + formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return new StringBuilder().append("X'").append(node.toHexString()).append("'").toString();
        }

        @Override
        protected String visitParameter(Parameter node, Void context)
        {
            if (!parameters.isPresent()) {
				return "?";
			}
			checkArgument(node.getPosition() < parameters.get().size(), "Invalid parameter number %s.  Max value is %s", node.getPosition(), parameters.get().size() - 1);
			return process(parameters.get().get(node.getPosition()), context);
        }

        @Override
        protected String visitArrayConstructor(ArrayConstructor node, Void context)
        {
            ImmutableList.Builder<String> valueStrings = ImmutableList.builder();
            node.getValues().forEach(value -> valueStrings.add(formatSql(value, parameters)));
            return new StringBuilder().append("ARRAY[").append(Joiner.on(",").join(valueStrings.build())).append("]").toString();
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            return new StringBuilder().append(formatSql(node.getBase(), parameters)).append("[").append(formatSql(node.getIndex(), parameters)).append("]").toString();
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context)
        {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return doubleFormatter.get().format(node.getValue());
        }

        @Override
        protected String visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            // TODO return node value without "DECIMAL '..'" when FeaturesConfig#parseDecimalLiteralsAsDouble switch is removed
            return new StringBuilder().append("DECIMAL '").append(node.getValue()).append("'").toString();
        }

        @Override
        protected String visitGenericLiteral(GenericLiteral node, Void context)
        {
            return new StringBuilder().append(node.getType()).append(" ").append(formatStringLiteral(node.getValue())).toString();
        }

        @Override
        protected String visitTimeLiteral(TimeLiteral node, Void context)
        {
            return new StringBuilder().append("TIME '").append(node.getValue()).append("'").toString();
        }

        @Override
        protected String visitTimestampLiteral(TimestampLiteral node, Void context)
        {
            return new StringBuilder().append("TIMESTAMP '").append(node.getValue()).append("'").toString();
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, Void context)
        {
            return "null";
        }

        @Override
        protected String visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            String sign = (node.getSign() == IntervalLiteral.Sign.NEGATIVE) ? "- " : "";
            StringBuilder builder = new StringBuilder()
                    .append("INTERVAL ")
                    .append(sign)
                    .append(" '").append(node.getValue()).append("' ")
                    .append(node.getStartField());

            node.getEndField().ifPresent(value -> builder.append(" TO ").append(value));
            return builder.toString();
        }

        @Override
        protected String visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            return new StringBuilder().append("(").append(formatSql(node.getQuery(), parameters)).append(")").toString();
        }

        @Override
        protected String visitExists(ExistsPredicate node, Void context)
        {
            return new StringBuilder().append("(EXISTS ").append(formatSql(node.getSubquery(), parameters)).append(")").toString();
        }

        @Override
        protected String visitIdentifier(Identifier node, Void context)
        {
            if (!node.isDelimited()) {
                return node.getValue();
            }
            else {
                return new StringBuilder().append('"').append(node.getValue().replace("\"", "\"\"")).append('"').toString();
            }
        }

        @Override
        protected String visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, Void context)
        {
            return formatExpression(node.getName(), parameters);
        }

        @Override
        protected String visitSymbolReference(SymbolReference node, Void context)
        {
            return formatIdentifier(node.getName());
        }

        @Override
        protected String visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            String baseString = process(node.getBase(), context);
            return new StringBuilder().append(baseString).append(".").append(process(node.getField())).toString();
        }

        @Override
        public String visitFieldReference(FieldReference node, Void context)
        {
            // add colon so this won't parse
            return new StringBuilder().append(":input(").append(node.getFieldIndex()).append(")").toString();
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            String arguments = joinExpressions(node.getArguments());
            if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
                arguments = "*";
            }
            if (node.isDistinct()) {
                arguments = "DISTINCT " + arguments;
            }

            builder.append(formatQualifiedName(node.getName()))
                    .append('(').append(arguments);

            node.getOrderBy().ifPresent(value -> builder.append(' ').append(formatOrderBy(value, parameters)));

            builder.append(')');

            node.getFilter().ifPresent(value -> builder.append(" FILTER ").append(visitFilter(value, context)));

            node.getWindow().ifPresent(value -> builder.append(" OVER ").append(visitWindow(value, context)));

            return builder.toString();
        }

        @Override
        protected String visitLambdaExpression(LambdaExpression node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append('(');
            Joiner.on(", ").appendTo(builder, node.getArguments());
            builder.append(") -> ");
            builder.append(process(node.getBody(), context));
            return builder.toString();
        }

        @Override
        protected String visitBindExpression(BindExpression node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("\"$INTERNAL$BIND\"(");
            node.getValues().forEach(value -> builder.append(process(value, context) + ", "));
            builder.append(process(node.getFunction(), context) + ")");
            return builder.toString();
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            return formatBinaryExpression(node.getOperator().toString(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitNotExpression(NotExpression node, Void context)
        {
            return new StringBuilder().append("(NOT ").append(process(node.getValue(), context)).append(")").toString();
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            return new StringBuilder().append("(").append(process(node.getValue(), context)).append(" IS NULL)").toString();
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            return new StringBuilder().append("(").append(process(node.getValue(), context)).append(" IS NOT NULL)").toString();
        }

        @Override
        protected String visitNullIfExpression(NullIfExpression node, Void context)
        {
            return new StringBuilder().append("NULLIF(").append(process(node.getFirst(), context)).append(", ").append(process(node.getSecond(), context)).append(')').toString();
        }

        @Override
        protected String visitIfExpression(IfExpression node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("IF(")
                    .append(process(node.getCondition(), context))
                    .append(", ")
                    .append(process(node.getTrueValue(), context));
            node.getFalseValue().ifPresent(value -> builder.append(", ")
			        .append(process(value, context)));
            builder.append(")");
            return builder.toString();
        }

        @Override
        protected String visitTryExpression(TryExpression node, Void context)
        {
            return new StringBuilder().append("TRY(").append(process(node.getInnerExpression(), context)).append(")").toString();
        }

        @Override
        protected String visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            return new StringBuilder().append("COALESCE(").append(joinExpressions(node.getOperands())).append(")").toString();
        }

        @Override
        protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            String value = process(node.getValue(), context);

            switch (node.getSign()) {
                case MINUS:
                    // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
                    String separator = value.startsWith("-") ? " " : "";
                    return new StringBuilder().append("-").append(separator).append(value).toString();
                case PLUS:
                    return "+" + value;
                default:
                    throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
            }
        }

        @Override
        protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append('(')
                    .append(process(node.getValue(), context))
                    .append(" LIKE ")
                    .append(process(node.getPattern(), context));

            node.getEscape().ifPresent(escape -> builder.append(" ESCAPE ").append(process(escape, context)));

            builder.append(')');

            return builder.toString();
        }

        @Override
        protected String visitAllColumns(AllColumns node, Void context)
        {
            if (node.getPrefix().isPresent()) {
                return node.getPrefix().get() + ".*";
            }

            return "*";
        }

        @Override
        public String visitCast(Cast node, Void context)
        {
            return new StringBuilder().append(node.isSafe() ? "TRY_CAST" : "CAST").append("(").append(process(node.getExpression(), context)).append(" AS ")
					.append(node.getType()).append(")").toString();
        }

        @Override
        protected String visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();
            parts.add("CASE");
            node.getWhenClauses().forEach(whenClause -> parts.add(process(whenClause, context)));

            node.getDefaultValue()
                    .ifPresent((value) -> parts.add("ELSE").add(process(value, context)));

            parts.add("END");

            return new StringBuilder().append("(").append(Joiner.on(' ').join(parts.build())).append(")").toString();
        }

        @Override
        protected String visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();

            parts.add("CASE")
                    .add(process(node.getOperand(), context));

            node.getWhenClauses().forEach(whenClause -> parts.add(process(whenClause, context)));

            node.getDefaultValue()
                    .ifPresent((value) -> parts.add("ELSE").add(process(value, context)));

            parts.add("END");

            return new StringBuilder().append("(").append(Joiner.on(' ').join(parts.build())).append(")").toString();
        }

        @Override
        protected String visitWhenClause(WhenClause node, Void context)
        {
            return new StringBuilder().append("WHEN ").append(process(node.getOperand(), context)).append(" THEN ").append(process(node.getResult(), context)).toString();
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            return new StringBuilder().append("(").append(process(node.getValue(), context)).append(" BETWEEN ").append(process(node.getMin(), context)).append(" AND ").append(process(node.getMax(), context))
					.append(")").toString();
        }

        @Override
        protected String visitInPredicate(InPredicate node, Void context)
        {
            return new StringBuilder().append("(").append(process(node.getValue(), context)).append(" IN ").append(process(node.getValueList(), context)).append(")").toString();
        }

        @Override
        protected String visitInListExpression(InListExpression node, Void context)
        {
            return new StringBuilder().append("(").append(joinExpressions(node.getValues())).append(")").toString();
        }

        private String visitFilter(Expression node, Void context)
        {
            return new StringBuilder().append("(WHERE ").append(process(node, context)).append(')').toString();
        }

        @Override
        public String visitWindow(Window node, Void context)
        {
            List<String> parts = new ArrayList<>();

            if (!node.getPartitionBy().isEmpty()) {
                parts.add("PARTITION BY " + joinExpressions(node.getPartitionBy()));
            }
            node.getOrderBy().ifPresent(value -> parts.add(formatOrderBy(value, parameters)));
            node.getFrame().ifPresent(value -> parts.add(process(value, context)));

            return new StringBuilder().append('(').append(Joiner.on(' ').join(parts)).append(')').toString();
        }

        @Override
        public String visitWindowFrame(WindowFrame node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getType().toString()).append(' ');

            if (node.getEnd().isPresent()) {
                builder.append("BETWEEN ")
                        .append(process(node.getStart(), context))
                        .append(" AND ")
                        .append(process(node.getEnd().get(), context));
            }
            else {
                builder.append(process(node.getStart(), context));
            }

            return builder.toString();
        }

        @Override
        public String visitFrameBound(FrameBound node, Void context)
        {
            switch (node.getType()) {
                case UNBOUNDED_PRECEDING:
                    return "UNBOUNDED PRECEDING";
                case PRECEDING:
                    return process(node.getValue().get(), context) + " PRECEDING";
                case CURRENT_ROW:
                    return "CURRENT ROW";
                case FOLLOWING:
                    return process(node.getValue().get(), context) + " FOLLOWING";
                case UNBOUNDED_FOLLOWING:
                    return "UNBOUNDED FOLLOWING";
            }
            throw new IllegalArgumentException("unhandled type: " + node.getType());
        }

        @Override
        protected String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Void context)
        {
            return new StringBuilder()
                    .append("(")
                    .append(process(node.getValue(), context))
                    .append(' ')
                    .append(node.getOperator().getValue())
                    .append(' ')
                    .append(node.getQuantifier().toString())
                    .append(' ')
                    .append(process(node.getSubquery(), context))
                    .append(")")
                    .toString();
        }

        @Override
		public String visitGroupingOperation(GroupingOperation node, Void context)
        {
            return new StringBuilder().append("GROUPING (").append(joinExpressions(node.getGroupingColumns())).append(")").toString();
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right)
        {
            return new StringBuilder().append('(').append(process(left, null)).append(' ').append(operator).append(' ').append(process(right, null))
					.append(')').toString();
        }

        private String joinExpressions(List<Expression> expressions)
        {
            return Joiner.on(", ").join(expressions.stream()
                    .map((e) -> process(e, null))
                    .iterator());
        }
    }
}
