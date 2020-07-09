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
package io.prestosql.plugin.kafka.encoder.raw;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.kafka.encoder.AbstractRowEncoder;
import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public class RawRowEncoder
        extends AbstractRowEncoder
{
    private enum FieldType
    {
        BYTE(Byte.SIZE),
        SHORT(Short.SIZE),
        INT(Integer.SIZE),
        LONG(Long.SIZE),
        FLOAT(Float.SIZE),
        DOUBLE(Double.SIZE);

        private final int size;

        FieldType(int bitSize)
        {
            this.size = bitSize / 8;
        }

        public int getSize()
        {
            return size;
        }
    }

    private static final Pattern MAPPING_PATTERN = Pattern.compile("(\\d+)(?::(\\d+))?");
    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, BOOLEAN);

    public static final String NAME = "raw";

    private final int[] valueLengths;
    private final ByteBuffer buffer;
    private int currentBufferPosition;

    public RawRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles)
    {
        super(session, columnHandles);

        valueLengths = new int[this.columnHandles.size()];
        int capacity = 0;
        EncoderColumnHandle columnHandle;

        for (int i = 0; i < this.columnHandles.size(); i++) {
            columnHandle = this.columnHandles.get(i);
            checkArgument(columnHandle.getFormatHint() == null, "Unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnHandle.getName());

            FieldType fieldType = parseFieldType(columnHandle);
            checkFieldType(columnHandle, fieldType);

            valueLengths[i] = parseMapping(columnHandle, fieldType);
            capacity += valueLengths[i];

            checkArgument(isSupportedType(columnHandle.getType()), "Unsupported column type '%s' for column '%s'", columnHandle.getType().getDisplayName(), columnHandle.getName());
        }

        this.buffer = ByteBuffer.allocate(capacity);
        this.currentBufferPosition = 0;
    }

    private FieldType parseFieldType(EncoderColumnHandle columnHandle)
    {
        try {
            return Optional.ofNullable(columnHandle.getDataFormat())
                    .map(dataFormat -> FieldType.valueOf(dataFormat.toUpperCase(Locale.ENGLISH)))
                    .orElse(FieldType.BYTE);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(format("Invalid dataFormat '%s' for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
        }
    }

    private void checkFieldType(EncoderColumnHandle columnHandle, FieldType fieldType)
    {
        String columnName = columnHandle.getName();
        Type columnType = columnHandle.getType();
        if (columnType == BIGINT) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE, FieldType.SHORT, FieldType.INT, FieldType.LONG);
        }
        else if (columnType == INTEGER) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE, FieldType.SHORT, FieldType.INT);
        }
        else if (columnType == SMALLINT) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE, FieldType.SHORT);
        }
        else if (columnType == TINYINT) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE);
        }
        else if (columnType == BOOLEAN) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE, FieldType.SHORT, FieldType.INT, FieldType.LONG);
        }
        else if (columnType == DOUBLE) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.DOUBLE, FieldType.FLOAT);
        }
        else if (isVarcharType(columnType)) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE);
        }
    }

    private void checkFieldTypeOneOf(FieldType declaredFieldType, String columnName, Type columnType, FieldType... allowedFieldTypes)
    {
        if (!Arrays.asList(allowedFieldTypes).contains(declaredFieldType)) {
            throw new IllegalArgumentException(format(
                    "Wrong dataFormat '%s' specified for column '%s'; %s type implies use of %s",
                    declaredFieldType.name(),
                    columnName,
                    columnType.getDisplayName(),
                    Joiner.on("/").join(allowedFieldTypes)));
        }
    }

    private int parseMapping(EncoderColumnHandle columnHandle, FieldType fieldType)
    {
        String mapping = Optional.ofNullable(columnHandle.getMapping()).orElse("0");
        Matcher mappingMatcher = MAPPING_PATTERN.matcher(mapping);
        if (!mappingMatcher.matches()) {
            throw new IllegalArgumentException(format("Invalid mapping format '%s' for column '%s'", mapping, columnHandle.getName()));
        }
        int start = parseInt(mappingMatcher.group(1));
        OptionalInt end;
        if (mappingMatcher.group(2) != null) {
            end = OptionalInt.of(parseInt(mappingMatcher.group(2)));
        }
        else {
            if (!isVarcharType(columnHandle.getType())) {
                end = OptionalInt.of(start + fieldType.getSize());
            }
            else {
                end = OptionalInt.empty();
            }
        }

        checkArgument(start >= 0, "Start offset %s for column '%s' must be greater or equal 0", start, columnHandle.getName());
        end.ifPresent(endValue -> {
            checkArgument(endValue >= 0, "End offset %s for column '%s' must be greater or equal 0", endValue, columnHandle.getName());
            checkArgument(endValue > start, "End offset %s for column '%s' must greater than start offset", endValue, columnHandle.getName());
        });

        int length = end.isPresent() ? end.getAsInt() - start : fieldType.getSize();

        if (!isVarcharType(columnHandle.getType())) {
            checkArgument(!end.isPresent() || end.getAsInt() - start == length,
                    "Bytes mapping for column '%s' does not match dataFormat '%s'; expected %s bytes but got %s",
                    columnHandle.getName(),
                    length,
                    end.getAsInt() - start);
        }

        return length;
    }

    private static boolean isSupportedType(Type type)
    {
        return isVarcharType(type) || SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    @Override
    protected void appendNullValue()
    {
        buffer.put(new byte[valueLengths[currentColumnIndex]], currentBufferPosition, valueLengths[currentColumnIndex]);
        currentBufferPosition += valueLengths[currentColumnIndex];
    }

    @Override
    protected void appendLong(long value)
    {
        buffer.putLong(currentBufferPosition, value);
        currentBufferPosition += valueLengths[currentColumnIndex];
    }

    @Override
    protected void appendInt(int value)
    {
        buffer.putInt(currentBufferPosition, value);
        currentBufferPosition += valueLengths[currentColumnIndex];
    }

    @Override
    protected void appendShort(short value)
    {
        buffer.putShort(currentBufferPosition, value);
        currentBufferPosition += valueLengths[currentColumnIndex];
    }

    @Override
    protected void appendByte(byte value)
    {
        buffer.put(currentBufferPosition, value);
        currentBufferPosition += valueLengths[currentColumnIndex];
    }

    @Override
    protected void appendDouble(double value)
    {
        buffer.putDouble(currentBufferPosition, value);
        currentBufferPosition += valueLengths[currentColumnIndex];
    }

    @Override
    protected void appendFloat(float value)
    {
        buffer.putFloat(currentBufferPosition, value);
        currentBufferPosition += valueLengths[currentColumnIndex];
    }

    @Override
    protected void appendBoolean(boolean value)
    {
        buffer.put(currentBufferPosition, (byte) (value ? 1 : 0));
        currentBufferPosition += valueLengths[currentColumnIndex];
    }

    @Override
    protected void appendString(String value)
    {
        buffer.put(value.getBytes(StandardCharsets.UTF_8), currentBufferPosition, valueLengths[currentColumnIndex]);
        currentBufferPosition += valueLengths[currentColumnIndex];
    }

    @Override
    protected void appendByteBuffer(ByteBuffer value)
    {
        buffer.put(value.array(), currentBufferPosition, valueLengths[currentColumnIndex]);
        currentBufferPosition += valueLengths[currentColumnIndex];
    }

    @Override
    public byte[] toByteArray()
    {
        // make sure entire row has been updated with new values
        checkArgument(currentColumnIndex == columnHandles.size(), format("Missing %d columns", columnHandles.size() - currentColumnIndex + 1));

        resetColumnIndex(); // reset currentColumnIndex to prepare for next row
        currentBufferPosition = 0;
        return buffer.array();
    }
}
