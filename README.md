# ArangoDB JDBC Driver

A JDBC driver implementation for ArangoDB that allows you to use standard JDBC APIs to interact with ArangoDB databases.

## Features

- Full JDBC 4.2 compliance (where applicable to ArangoDB)
- Support for AQL (ArangoDB Query Language) queries
- Prepared statements with named parameter binding (@paramName)
- Result set navigation and data retrieval
- Database metadata support
- Connection pooling ready

## Requirements

- Java 17 or higher
- ArangoDB 3.x or higher
- Maven 3.6 or higher

## Installation

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.iotahoe</groupId>
    <artifactId>arango-jdbc</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Manual Installation

1. Clone this repository
2. Build the project: `mvn clean install`
3. Add the JAR file to your classpath

## Usage

### Basic Connection

```java
import java.sql.*;

public class ArangoJdbcExample {
    public static void main(String[] args) {
        String url = "jdbc:arangodb://localhost:8529/_system";
        String user = "root";
        String password = "";
        
        try {
            // Register the driver
            Class.forName("com.iotahoe.jdbc.ArangoDbDriver");
            
            // Create connection
            Connection connection = DriverManager.getConnection(url, user, password);
            
            // Create statement
            Statement statement = connection.createStatement();
            
            // Execute AQL query
            String sql = "FOR i IN 1..5 RETURN { value: i, doubled: i * 2 }";
            ResultSet resultSet = statement.executeQuery(sql);
            
            // Process results
            while (resultSet.next()) {
                int value = resultSet.getInt("value");
                int doubled = resultSet.getInt("doubled");
                System.out.println("Value: " + value + ", Doubled: " + doubled);
            }
            
            // Clean up
            resultSet.close();
            statement.close();
            connection.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### Prepared Statements

ArangoDB only supports named parameters (`@paramName`). The JDBC driver maps positional setter methods to named parameters based on their order in the query.

#### Using Named Parameters Directly

```java
String sql = "FOR t IN firstCollection FILTER t.name == @name AND t.age > @minAge RETURN t";
PreparedStatement preparedStatement = connection.prepareStatement(sql);

// Cast to ArangoDbPreparedStatement to use named parameters directly
if (preparedStatement instanceof ArangoDbPreparedStatement) {
    ArangoDbPreparedStatement arangoStmt = (ArangoDbPreparedStatement) preparedStatement;
    arangoStmt.setParameter("name", "John Doe");
    arangoStmt.setParameter("minAge", 25);
    ResultSet resultSet = arangoStmt.executeQuery();
}
```

#### Using Positional Setter Methods (Mapped to Named Parameters)

```java
String sql = "FOR t IN firstCollection FILTER t.name == @name AND t.age > @minAge RETURN t";
PreparedStatement preparedStatement = connection.prepareStatement(sql);

// Use standard JDBC positional setters - they map to named parameters by order
preparedStatement.setString(1, "John Doe");  // Maps to @name (first parameter)
preparedStatement.setInt(2, 25);            // Maps to @minAge (second parameter)
ResultSet resultSet = preparedStatement.executeQuery();
```

#### Complex Queries with Named Parameters

```java
String sql = "FOR t IN firstCollection " +
            "FILTER t.name == @name " +
            "REMOVE t IN firstCollection " +
            "LET removed = OLD " +
            "RETURN removed";
PreparedStatement preparedStatement = connection.prepareStatement(sql);

// Using positional setters
preparedStatement.setString(1, "John Doe");  // Maps to @name
int updateCount = preparedStatement.executeUpdate();

// Or using named parameters directly
if (preparedStatement instanceof ArangoDbPreparedStatement) {
    ArangoDbPreparedStatement arangoStmt = (ArangoDbPreparedStatement) preparedStatement;
    arangoStmt.setParameter("name", "John Doe");
    int updateCount = arangoStmt.executeUpdate();
}
```

### URL Format

The JDBC URL format is:
```
jdbc:arangodb://host:port/database
```

Examples:
- `jdbc:arangodb://localhost:8529/_system`
- `jdbc:arangodb://192.168.1.100:8529/mydb`
- `jdbc:arangodb://arangodb.example.com:8529/production`

## Supported Features

### Supported
- Basic AQL queries (SELECT-like operations)
- Prepared statements with parameter binding
- Result set navigation (forward-only)
- Basic data type mapping
- Connection management
- Database metadata (limited)

### Not Supported
- Transactions (ArangoDB handles this differently)
- Batch operations
- Stored procedures
- Multiple result sets
- Scrollable result sets
- Updatable result sets
- BLOB/CLOB operations
- Array operations
- Savepoints

## Data Type Mapping

| ArangoDB Type | JDBC Type | Java Type |
|---------------|-----------|-----------|
| String | VARCHAR | String |
| Number (Integer) | INTEGER | Integer |
| Number (Long) | BIGINT | Long |
| Number (Double) | DOUBLE | Double |
| Boolean | BOOLEAN | Boolean |
| Date | TIMESTAMP | Timestamp |
| Null | NULL | null |

## Building from Source

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd arango-jdbc
   ```

2. Build the project:
   ```bash
   mvn clean install
   ```

3. To build the shaded JAR with all dependencies, use the `shaded-jar` profile:
   ```bash
   mvn clean package -P shaded-jar
   ```

4. Run tests:
   ```bash
   mvn test
   ```

## Testing

Make sure you have an ArangoDB instance running on `localhost:8529` before running the tests:

```bash
mvn test
```

## Limitations

- This driver is designed for read operations and basic AQL queries
- Complex ArangoDB features like transactions, graphs, and advanced AQL features may not be fully supported
- Performance may not be optimal for large datasets compared to native ArangoDB drivers
- Some JDBC features are not applicable to ArangoDB and are not implemented

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Support

For issues and questions, please create an issue in the GitHub repository.
