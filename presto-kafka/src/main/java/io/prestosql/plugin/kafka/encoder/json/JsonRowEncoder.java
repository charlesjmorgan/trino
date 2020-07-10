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
package io.prestosql.plugin.kafka.encoder.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.kafka.encoder.AbstractRowEncoder;
import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.SqlTimeWithTimeZone;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JsonRowEncoder
        extends AbstractRowEncoder
{
    private static final Set<Type> PRIMITIVE_SUPPORTED_TYPES = ImmutableSet.of(
            BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, BOOLEAN);
    private static final Set<Type> NON_PARAMETRIC_DATE_TIME_TYPES = ImmutableSet.of(
            DATE, TIME, TIME_WITH_TIME_ZONE);
    private static final Set<String> DATE_TIME_FORMATS = ImmutableSet.of(
            "custom-date-time", "iso8601", "rfc2822", "milliseconds-since-epoch", "seconds-since-epoch");
    private static final String CUSTOM_DATE_TIME_NAME = "custom-date-time";
    private static final DateTimeFormatter RFC_FORMATTER = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss X yyyy").withLocale(Locale.ENGLISH).withZone(UTC);

    public static final String NAME = "json";

    private final ObjectMapper objectMapper;
    private final ObjectNode node;

    JsonRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles, ObjectMapper objectMapper)
    {
        super(session, columnHandles);
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.node = objectMapper.createObjectNode();
    }

    @Override
    protected void validateColumns(List<EncoderColumnHandle> columnHandles)
    {
        for (EncoderColumnHandle columnHandle : columnHandles) {
            if (!isDateTimeType(columnHandle.getType())) {
                checkArgument(columnHandle.getFormatHint() == null, "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnHandle.getName());
                checkArgument(columnHandle.getDataFormat() == null, "unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName());
            }
            else {
                checkArgument(DATE_TIME_FORMATS.contains(columnHandle.getDataFormat()), "incorrect data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName());
                if (columnHandle.getDataFormat().equals(CUSTOM_DATE_TIME_NAME)) {
                    checkArgument(columnHandle.getFormatHint() != null, "no format hint defined for column '%s'", columnHandle.getName());
                }
                else {
                    checkArgument(columnHandle.getFormatHint() == null, "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnHandle.getName());
                }
            }

            checkArgument(isSupportedType(columnHandle.getType()), "unsupported column type '%s' for column '%s'", columnHandle.getType(), columnHandle.getName());
        }
    }

    private boolean isSupportedType(Type type)
    {
        return isVarcharType(type) ||
                type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType ||
                PRIMITIVE_SUPPORTED_TYPES.contains(type) ||
                NON_PARAMETRIC_DATE_TIME_TYPES.contains(type);
    }

    private boolean isDateTimeType(Type type)
    {
        return type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType ||
                NON_PARAMETRIC_DATE_TIME_TYPES.contains(type);
    }

    @Override
    protected void appendNullValue()
    {
        node.putNull(columnHandles.get(currentColumnIndex).getName());
    }

    @Override
    protected void appendLong(long value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendInt(int value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendShort(short value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendByte(byte value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendDouble(double value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendFloat(float value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendBoolean(boolean value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendString(String value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendByteBuffer(ByteBuffer value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), value.array());
    }

    @Override
    protected void appendSqlDate(SqlDate value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), this.formatDateTime(columnHandles.get(currentColumnIndex), value));
    }

    @Override
    protected void appendSqlTime(SqlTime value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), this.formatDateTime(columnHandles.get(currentColumnIndex), value));
    }

    @Override
    protected void appendSqlTimeWithTimeZone(SqlTimeWithTimeZone value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), this.formatDateTime(columnHandles.get(currentColumnIndex), value));
    }

    @Override
    protected void appendSqlTimestamp(SqlTimestamp value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), this.formatDateTime(columnHandles.get(currentColumnIndex), value));
    }

    @Override
    protected void appendSqlTimestampWithTimeZone(SqlTimestampWithTimeZone value)
    {
        node.put(columnHandles.get(currentColumnIndex).getName(), this.formatDateTime(columnHandles.get(currentColumnIndex), value));
    }

    private org.joda.time.format.DateTimeFormatter getCustomFormatter(String format)
    {
        try {
            return org.joda.time.format.DateTimeFormat.forPattern(format).withLocale(Locale.ENGLISH);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(
                    GENERIC_USER_ERROR,
                    format("invalid joda pattern '%s' passed as format hint", format));
        }
    }

    private String formatDateTime(EncoderColumnHandle columnHandle, SqlDate value)
    {
        switch (columnHandle.getDataFormat()) {
            case "custom-date-time":
                return this.getCustomFormatter(columnHandle.getFormatHint())
                        .print(org.joda.time.Instant.ofEpochMilli(DAYS.toMillis(value.getDays())).toDateTime());
            case "rfc2822":
                return Instant.ofEpochMilli(DAYS.toMillis(value.getDays()))
                        .atZone(UTC)
                        .format(RFC_FORMATTER);
            case "iso8601":
                return Instant.ofEpochMilli(DAYS.toMillis(value.getDays()))
                        .atZone(UTC)
                        .toLocalDate()
                        .format(ISO_DATE);
            case "milliseconds-since-epoch":
                return String.valueOf(DAYS.toMillis(value.getDays()));
            case "seconds-since-epoch":
                return String.valueOf(MILLISECONDS.toSeconds(DAYS.toMillis(value.getDays())));
            default:
                throw new PrestoException(GENERIC_USER_ERROR, format("invalid data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
        }
    }

    private String formatDateTime(EncoderColumnHandle columnHandle, SqlTime value)
    {
        switch (columnHandle.getDataFormat()) {
            case "custom-date-time":
                return this.getCustomFormatter(columnHandle.getFormatHint())
                        .print(org.joda.time.Instant.ofEpochMilli(value.getMillis()).toDateTime());
            case "rfc2822":
                return Instant.ofEpochMilli(value.getMillis())
                        .atZone(UTC)
                        .format(RFC_FORMATTER);
            case "iso8601":
                return Instant.ofEpochMilli(value.getMillis())
                        .atZone(UTC)
                        .toLocalDateTime()
                        .format(ISO_DATE_TIME);
            case "milliseconds-since-epoch":
                return String.valueOf(value.getMillis());
            case "seconds-since-epoch":
                return String.valueOf(MILLISECONDS.toSeconds(value.getMillis()));
            default:
                throw new PrestoException(GENERIC_USER_ERROR, format("invalid data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
        }
    }

    private String formatDateTime(EncoderColumnHandle columnHandle, SqlTimeWithTimeZone value)
    {
        switch (columnHandle.getDataFormat()) {
            case "custom-date-time":
                return this.getCustomFormatter(columnHandle.getFormatHint())
                        .print(org.joda.time.Instant.ofEpochMilli(value.getMillisUtc()).toDateTime());
            case "rfc2822":
                return Instant.ofEpochMilli(value.getMillisUtc())
                        .atZone(UTC)
                        .format(RFC_FORMATTER);
            case "iso8601":
                return Instant.ofEpochMilli(value.getMillisUtc())
                        .atZone(UTC)
                        .toLocalDateTime()
                        .format(ISO_DATE_TIME);
            case "milliseconds-since-epoch":
                return String.valueOf(value.getMillisUtc());
            case "seconds-since-epoch":
                return String.valueOf(MILLISECONDS.toSeconds(value.getMillisUtc()));
            default:
                throw new PrestoException(GENERIC_USER_ERROR, format("invalid data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
        }
    }

    private String formatDateTime(EncoderColumnHandle columnHandle, SqlTimestamp value)
    {
        if (!value.isLegacyTimestamp()) {
            switch (columnHandle.getDataFormat()) {
                case "custom-date-time":
                    return this.getCustomFormatter(columnHandle.getFormatHint())
                            .print(org.joda.time.Instant.ofEpochMilli(value.getMillis()).toDateTime());
                case "rfc2822":
                    return Instant.ofEpochMilli(value.getMillis())
                            .atZone(UTC)
                            .format(RFC_FORMATTER);
                case "iso8601":
                    return Instant.ofEpochMilli(value.getMillis())
                            .atZone(UTC)
                            .toLocalDateTime()
                            .format(ISO_DATE_TIME);
                case "milliseconds-since-epoch":
                    return String.valueOf(value.getMillis());
                case "seconds-since-epoch":
                    return String.valueOf(MILLISECONDS.toSeconds(value.getMillis()));
                default:
                    throw new PrestoException(GENERIC_USER_ERROR, format("invalid data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
            }
        }
        else {
            switch (columnHandle.getDataFormat()) {
                case "custom-date-time":
                    return this.getCustomFormatter(columnHandle.getFormatHint())
                            .print(org.joda.time.Instant.ofEpochMilli(value.getMillisUtc()).toDateTime());
                case "rfc2822":
                    return Instant.ofEpochMilli(value.getMillisUtc())
                            .atZone(UTC)
                            .format(RFC_FORMATTER);
                case "iso8601":
                    return Instant.ofEpochMilli(value.getMillisUtc())
                            .atZone(UTC)
                            .toLocalDateTime()
                            .format(ISO_DATE_TIME);
                case "milliseconds-since-epoch":
                    return String.valueOf(value.getMillisUtc());
                case "seconds-since-epoch":
                    return String.valueOf(MILLISECONDS.toSeconds(value.getMillisUtc()));
                default:
                    throw new PrestoException(GENERIC_USER_ERROR, format("invalid data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
            }
        }
    }

    private String formatDateTime(EncoderColumnHandle columnHandle, SqlTimestampWithTimeZone value)
    {
        switch (columnHandle.getDataFormat()) {
            case "custom-date-time":
                return this.getCustomFormatter(columnHandle.getFormatHint())
                        .print(org.joda.time.Instant.ofEpochMilli(value.getMillisUtc()).toDateTime());
            case "rfc2822":
                return Instant.ofEpochMilli(value.getMillisUtc())
                        .atZone(UTC)
                        .format(RFC_FORMATTER);
            case "iso8601":
                return Instant.ofEpochMilli(value.getMillisUtc())
                        .atZone(UTC)
                        .toLocalDateTime()
                        .format(ISO_DATE_TIME);
            case "milliseconds-since-epoch":
                return String.valueOf(value.getMillisUtc());
            case "seconds-since-epoch":
                return String.valueOf(MILLISECONDS.toSeconds(value.getMillisUtc()));
            default:
                throw new PrestoException(GENERIC_USER_ERROR, format("invalid data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
        }
    }

    @Override
    public byte[] toByteArray()
    {
        // make sure entire row has been updated with new values
        checkArgument(currentColumnIndex == columnHandles.size(), format("Missing %d columns", columnHandles.size() - currentColumnIndex + 1));

        try {
            resetColumnIndex(); // reset currentColumnIndex to prepare for next row
            return objectMapper.writeValueAsBytes(node);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
