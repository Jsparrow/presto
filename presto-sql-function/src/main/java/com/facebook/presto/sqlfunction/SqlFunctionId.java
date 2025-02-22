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
package com.facebook.presto.sqlfunction;

import com.facebook.presto.spi.relation.FullyQualifiedName;
import com.facebook.presto.spi.type.TypeSignature;

import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class SqlFunctionId
{
    private final FullyQualifiedName name;
    private final List<TypeSignature> argumentTypes;

    public SqlFunctionId(
            FullyQualifiedName name,
            List<TypeSignature> argumentTypes)
    {
        this.name = requireNonNull(name, "name is null");
        this.argumentTypes = requireNonNull(argumentTypes, "argumentTypes is null");
    }

    public FullyQualifiedName getName()
    {
        return name;
    }

    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SqlFunctionId o = (SqlFunctionId) obj;
        return Objects.equals(name, o.name)
                && Objects.equals(argumentTypes, o.argumentTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, argumentTypes);
    }

    @Override
    public String toString()
    {
        String arguments = argumentTypes.stream()
                .map(Object::toString)
                .collect(joining(", "));
        return format("%s(%s)", name, arguments);
    }
}
