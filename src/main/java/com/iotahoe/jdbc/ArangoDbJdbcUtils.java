package com.iotahoe.jdbc;

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
}
