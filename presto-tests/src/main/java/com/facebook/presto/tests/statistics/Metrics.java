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
package com.facebook.presto.tests.statistics;

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.VariableStatsEstimate;

import java.util.Optional;
import java.util.OptionalDouble;

import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;

public final class Metrics
{
    public static final Metric OUTPUT_ROW_COUNT = new Metric()
    {
        @Override
        public OptionalDouble getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
        {
            return asOptional(planNodeStatsEstimate.getOutputRowCount());
        }

        @Override
        public OptionalDouble getValueFromAggregationQueryResult(Object value)
        {
            return OptionalDouble.of(((Number) value).doubleValue());
        }

        @Override
        public String getComputingAggregationSql()
        {
            return "count(*)";
        }

        @Override
        public String toString()
        {
            return "OUTPUT_ROW_COUNT";
        }
    };

	private Metrics() {}

	public static Metric nullsFraction(String columnName)
    {
        return new Metric()
        {
            @Override
            public OptionalDouble getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                return asOptional(getVariableStatistics(planNodeStatsEstimate, columnName, statsContext).getNullsFraction());
            }

            @Override
            public OptionalDouble getValueFromAggregationQueryResult(Object value)
            {
                return OptionalDouble.of(((Number) value).doubleValue());
            }

            @Override
            public String getComputingAggregationSql()
            {
                return new StringBuilder().append("(count(*) filter(where ").append(columnName).append(" is null)) / cast(count(*) as double)").toString();
            }

            @Override
            public String toString()
            {
                return new StringBuilder().append("nullsFraction(\"").append(columnName).append("\")").toString();
            }
        };
    }

	public static Metric distinctValuesCount(String columnName)
    {
        return new Metric()
        {
            @Override
            public OptionalDouble getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                return asOptional(getVariableStatistics(planNodeStatsEstimate, columnName, statsContext).getDistinctValuesCount());
            }

            @Override
            public OptionalDouble getValueFromAggregationQueryResult(Object value)
            {
                return OptionalDouble.of(((Number) value).doubleValue());
            }

            @Override
            public String getComputingAggregationSql()
            {
                return new StringBuilder().append("count(distinct ").append(columnName).append(")").toString();
            }

            @Override
            public String toString()
            {
                return new StringBuilder().append("distinctValuesCount(\"").append(columnName).append("\")").toString();
            }
        };
    }

	public static Metric lowValue(String columnName)
    {
        return new Metric()
        {
            @Override
            public OptionalDouble getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                double lowValue = getVariableStatistics(planNodeStatsEstimate, columnName, statsContext).getLowValue();
                if (isInfinite(lowValue)) {
                    return OptionalDouble.empty();
                }
                return OptionalDouble.of(lowValue);
            }

            @Override
            public OptionalDouble getValueFromAggregationQueryResult(Object value)
            {
                return Optional.ofNullable(value)
                        .map(Number.class::cast)
                        .map(Number::doubleValue)
                        .map(OptionalDouble::of)
                        .orElseGet(OptionalDouble::empty);
            }

            @Override
            public String getComputingAggregationSql()
            {
                return new StringBuilder().append("try_cast(min(").append(columnName).append(") as double)").toString();
            }

            @Override
            public String toString()
            {
                return new StringBuilder().append("lowValue(\"").append(columnName).append("\")").toString();
            }
        };
    }

	public static Metric highValue(String columnName)
    {
        return new Metric()
        {
            @Override
            public OptionalDouble getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                double highValue = getVariableStatistics(planNodeStatsEstimate, columnName, statsContext).getHighValue();
                if (isInfinite(highValue)) {
                    return OptionalDouble.empty();
                }
                return OptionalDouble.of(highValue);
            }

            @Override
            public OptionalDouble getValueFromAggregationQueryResult(Object value)
            {
                return Optional.ofNullable(value)
                        .map(Number.class::cast)
                        .map(Number::doubleValue)
                        .map(OptionalDouble::of)
                        .orElseGet(OptionalDouble::empty);
            }

            @Override
            public String getComputingAggregationSql()
            {
                return new StringBuilder().append("max(try_cast(").append(columnName).append(" as double))").toString();
            }

            @Override
            public String toString()
            {
                return new StringBuilder().append("highValue(\"").append(columnName).append("\")").toString();
            }
        };
    }

	private static VariableStatsEstimate getVariableStatistics(PlanNodeStatsEstimate planNodeStatsEstimate, String columnName, StatsContext statsContext)
    {
        return planNodeStatsEstimate.getVariableStatistics(statsContext.getVariableForColumn(columnName));
    }

	private static OptionalDouble asOptional(double value)
    {
        return isNaN(value) ? OptionalDouble.empty() : OptionalDouble.of(value);
    }
}
