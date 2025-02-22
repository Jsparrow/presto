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
package com.facebook.presto.sql.planner;

import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.TryExpression;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class NullabilityAnalyzer
{
    private final FunctionManager functionManager;
    private final TypeManager typeManager;

    public NullabilityAnalyzer(FunctionManager functionManager, TypeManager typeManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    /**
     * TODO: this currently produces a very conservative estimate.
     * We need to narrow down the conditions under which certain constructs
     * can return null (e.g., if(a, b, c) might return null for non-null a
     * only if b or c can be null.
     */
    @Deprecated
    public static boolean mayReturnNullOnNonNullInput(Expression expression)
    {
        requireNonNull(expression, "expression is null");

        AtomicBoolean result = new AtomicBoolean(false);
        new Visitor().process(expression, result);
        return result.get();
    }

    public boolean mayReturnNullOnNonNullInput(RowExpression expression)
    {
        requireNonNull(expression, "expression is null");

        AtomicBoolean result = new AtomicBoolean(false);
        expression.accept(new RowExpressionVisitor(functionManager, typeManager), result);
        return result.get();
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<Void, AtomicBoolean>
    {
        @Override
        protected Void visitCast(Cast node, AtomicBoolean result)
        {
            // Certain casts (e.g., try_cast, cast(JSON 'null' AS ...)) can return
            // null on non-null input.
            // Type only casts are not evaluated by the execution, thus cannot return
            // null on non-null input.
            // TODO: This should be a part of a cast operator metadata
            result.set(node.isSafe() || !node.isTypeOnly());
            return null;
        }

        @Override
        protected Void visitNullIfExpression(NullIfExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitInPredicate(InPredicate node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitSearchedCaseExpression(SearchedCaseExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitSimpleCaseExpression(SimpleCaseExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitSubscriptExpression(SubscriptExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitTryExpression(TryExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitIfExpression(IfExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, AtomicBoolean result)
        {
            // TODO: this should look at whether the return type of the function is annotated with @SqlNullable
            result.set(true);
            return null;
        }
    }

    private static class RowExpressionVisitor
            extends DefaultRowExpressionTraversalVisitor<AtomicBoolean>
    {
        private final FunctionManager functionManager;
        private final TypeManager typeManager;

        public RowExpressionVisitor(FunctionManager functionManager, TypeManager typeManager)
        {
            this.functionManager = functionManager;
            this.typeManager = typeManager;
        }

        @Override
        public Void visitCall(CallExpression call, AtomicBoolean result)
        {
            FunctionMetadata function = functionManager.getFunctionMetadata(call.getFunctionHandle());
            Optional<OperatorType> operator = function.getOperatorType();
            if (operator.isPresent()) {
                switch (operator.get()) {
                    case SATURATED_FLOOR_CAST:
                    case CAST: {
                        checkArgument(call.getArguments().size() == 1);
                        Type sourceType = call.getArguments().get(0).getType();
                        Type targetType = call.getType();
                        if (!typeManager.isTypeOnlyCoercion(sourceType, targetType)) {
                            result.set(true);
                        }
                    }
                    case SUBSCRIPT:
                        result.set(true);
                }
            }
            else if (!functionReturnsNullForNotNullInput(function)) {
                // TODO: use function annotation instead of assume all function can return NULL
                result.set(true);
            }
            call.getArguments().forEach(argument -> argument.accept(this, result));
            return null;
        }

        private boolean functionReturnsNullForNotNullInput(FunctionMetadata function)
        {
            return (function.getName().getSuffix().equalsIgnoreCase("like"));
        }

        @Override
        public Void visitSpecialForm(SpecialFormExpression specialForm, AtomicBoolean result)
        {
            switch (specialForm.getForm()) {
                case IN:
                case IF:
                case SWITCH:
                case WHEN:
                case NULL_IF:
                case DEREFERENCE:
                    result.set(true);
            }
            specialForm.getArguments().forEach(argument -> argument.accept(this, result));
            return null;
        }
    }
}
