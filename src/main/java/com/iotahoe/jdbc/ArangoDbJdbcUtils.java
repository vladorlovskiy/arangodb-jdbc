package com.iotahoe.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

public final class ArangoDbJdbcUtils {

    private static final String REDACTED_VALUE = "*REDACTED*";

    private ArangoDbJdbcUtils() {
        // private constructor to prevent instantiation
    }

    public static Map<Object, Object> redactProperties(final Properties properties) {
        return Optional.ofNullable(properties)
            .orElseGet(Properties::new)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    String key = (String) entry.getKey();
                    if (ArangoDbConstants.PROPERTY_PASSWORD.equals(key)) {
                        return REDACTED_VALUE;
                    }
                    if (ArangoDbConstants.PROPERTY_JWT.equals(key)) {
                        return REDACTED_VALUE;
                    }
                    return entry.getValue();
                }
            ));
    }

    public static int getSqlType(final Object value) {
        if (value == null) {
            return Types.NULL;
        } else if (value instanceof String) {
            return Types.VARCHAR;
        } else if (value instanceof Integer) {
            return Types.INTEGER;
        } else if (value instanceof Long) {
            return Types.BIGINT;
        } else if (value instanceof Double || value instanceof Float) {
            return Types.DOUBLE;
        } else if (value instanceof Boolean) {
            return Types.BOOLEAN;
        } else if (value instanceof java.util.Date) {
            return Types.TIMESTAMP;
        } else {
            return Types.OTHER;
        }
    }

    public static String asString(final Object value) {
        return value != null ? value.toString() : null;
    }

    public static boolean asBoolean(final Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        return false;
    }

    public static byte asByte(final Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }
        if (value instanceof String) {
            return Byte.parseByte((String) value);
        }
        return 0;
    }

    public static short asShort(final Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        if (value instanceof String) {
            return Short.parseShort((String) value);
        }
        return 0;
    }

    public static int asInt(final Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        return 0;
    }

    public static long asLong(final Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        return 0;
    }

    public static float asFloat(final Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        if (value instanceof String) {
            return Float.parseFloat((String) value);
        }
        return 0;
    }

    public static double asDouble(final Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        return 0;
    }

    public static BigDecimal asBigDecimal(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return new BigDecimal(((Number) value).doubleValue());
        }
        if (value instanceof String) {
            return new BigDecimal((String) value);
        }
        return null;
    }

    public static byte[] asBytes(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof String) {
            return ((String) value).getBytes();
        }
        return null;
    }

    public static Date asDate(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return (Date) value;
        }
        if (value instanceof java.util.Date) {
            return new Date(((java.util.Date) value).getTime());
        }
        if (value instanceof String) {
            return Date.valueOf((String) value);
        }
        return null;
    }

    public static Time asTime(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Time) {
            return (Time) value;
        }
        if (value instanceof java.util.Date) {
            return new Time(((java.util.Date) value).getTime());
        }
        if (value instanceof String) {
            return Time.valueOf((String) value);
        }
        return null;
    }

    public static Timestamp asTimestamp(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        }
        if (value instanceof String) {
            return Timestamp.valueOf((String) value);
        }
        return null;
    }
}
