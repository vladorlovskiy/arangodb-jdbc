package com.iotahoe.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.ArangoDBVersion;
import com.arangodb.entity.CollectionType;
import com.arangodb.entity.ViewType;

/**
 * ArangoDB JDBC DatabaseMetaData implementation.
 * This class implements the java.sql.DatabaseMetaData interface for ArangoDB.
 */
public class ArangoDbDatabaseMetaData implements DatabaseMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDbDatabaseMetaData.class);
    private final ArangoDbConnection connection;
    public ArangoDbDatabaseMetaData(ArangoDbConnection connection) {
        LOGGER.debug("ArangoDbDatabaseMetaData(connection={})", connection);
        this.connection = connection;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        LOGGER.debug("getCatalogs()");
        try {
            ArangoDB arangoDB = connection.getArangoDB();
            List<Map<String, Object>> data = arangoDB
                .getAccessibleDatabases()
                .stream()
                .filter(dbName -> !arangoDB.db(dbName).getInfo().getIsSystem())
                .map(dbName -> {
                    Map<String, Object> row = new HashMap<>();
                    row.put("TABLE_CAT", dbName);
                    return row;
                })
                .collect(Collectors.toList());
            return new ArangoDbListResultSet<>(connection.createStatement(), data);
        } catch (Exception e) {
            e.printStackTrace();
            return new ArangoDbListResultSet<>(connection.createStatement(), new ArrayList<>());
        }
    }


    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        LOGGER.debug("getTables(catalog={}, schemaPattern={}, tableNamePattern={}, types={})", new Object[]{catalog, schemaPattern, tableNamePattern, types});
        String dbName = catalog == null || catalog.isEmpty() ? connection.getCatalog() : catalog;
        LOGGER.debug("Fetching collections from database: {}", dbName);
        ArangoDatabase db = connection.getArangoDB().db(dbName);
        String connSchema = connection.getSchema();

        List<Map<String, Object>> tables = new ArrayList<>();
        db.getCollections().forEach(entity -> {
            if(entity.getIsSystem()) {
                return;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("TABLE_CAT", dbName);
            row.put("TABLE_SCHEM", connSchema);
            row.put("TABLE_NAME", entity.getName());
            row.put("TABLE_TYPE", entity.getType().toString());
            row.put("TABLE_REMARKS", "");
            row.put("TYPE_CAT", null);
            row.put("TYPE_SCHEM", null);
            row.put("TYPE_NAME", null);
            row.put("SELF_REFERENCING_COL_NAME", null);
            row.put("REF_GENERATION", null);
            row.put("ARANGODB_ENTITY", entity);
            tables.add(row);
        });
        
        db.getViews().forEach(entity -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("TABLE_CAT", dbName);
            row.put("TABLE_SCHEM", connSchema);
            row.put("TABLE_NAME", entity.getName());
            row.put("TABLE_TYPE", entity.getType().toString());
            row.put("TABLE_REMARKS", "");
            row.put("TYPE_CAT", null);
            row.put("TYPE_SCHEM", null);
            row.put("TYPE_NAME", null);
            row.put("SELF_REFERENCING_COL_NAME", null);
            row.put("REF_GENERATION", null);
            row.put("ARANGODB_ENTITY", entity);
            tables.add(row);
        });
        return new ArangoDbListResultSet<Object>(connection.createStatement(), tables);
    }
    
    @Override
    public ResultSet getTableTypes() throws SQLException {
        LOGGER.debug("getTableTypes()");
        var tableTypes = Stream.of(
            CollectionType.DOCUMENT, 
            CollectionType.EDGES,
            ViewType.ARANGO_SEARCH,
            ViewType.SEARCH_ALIAS
        ).map(type->Collections.singletonMap("TABLE_TYPE", type.toString()))
        .collect(Collectors.toList());

        return new ArangoDbListResultSet<>(connection.createStatement(), tableTypes);
    }


    private int getSqlDataType(String columnTypeName) {
        switch (columnTypeName) {
            case "bool":
                return Types.BOOLEAN;
            case "number":
                return Types.DOUBLE;
            case "string":
                return Types.VARCHAR;
            case "array":
                return Types.ARRAY;
            case "object":
                return Types.STRUCT;
            default:
                return Types.VARCHAR;
        }
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        LOGGER.debug("getColumns(catalog={}, schemaPattern={}, tableNamePattern={}, columnNamePattern={})", new Object[]{catalog, schemaPattern, tableNamePattern, columnNamePattern});
        String dbName = catalog == null || catalog.isEmpty() ? connection.getCatalog() : catalog;
        ArangoDatabase db = connection.getArangoDB().db(dbName);
        LOGGER.debug("Fetching collections from database: {}", dbName);
        String connSchema = connection.getSchema();
        int metadataSampleSize = connection.getJdbcMetadataSampleSize();

        List<Map<String, Object>> columns = new ArrayList<>();
        db.getCollections().forEach(collection -> {
            if (collection.getIsSystem()) {
                return;
            }
            String collectionName = collection.getName();
            if(tableNamePattern != null && !collectionName.equals(tableNamePattern)) {
                return;
            }
            LOGGER.debug("Fetching columns from collection: {}", collectionName);
            String query =String.format("""
                    RETURN UNIQUE(
                        FOR doc IN `%s`
                            FOR attr in ATTRIBUTES(doc, true)
                                LET value = doc[attr]
                                FILTER !IS_NULL(value)
                                %s
                                RETURN {attr, type: TYPENAME(value)}
                    )
                """, 
                collectionName, 
                metadataSampleSize > 0 ? "LIMIT " + metadataSampleSize : ""
                );
                
                @SuppressWarnings({ "rawtypes" })
                ArangoCursor<List> cursor = db.query(query, List.class);
                if (cursor.hasNext()) {
                    int ordinalPosition = 0;
                    @SuppressWarnings("unchecked")
                    List<Map<String, String>> innerList = cursor.next();
                    for (Map<String, String> entry : innerList) {
                        String columnName = entry.get("attr");
                        String columnTypeName = entry.get("type");
                        LOGGER.debug("Column name: {}, column type name: {}, column type: {}", columnName, columnTypeName, getSqlDataType(columnTypeName));
                        Map<String, Object> row = new LinkedHashMap<>();
                        ordinalPosition++;
                        row.put("TABLE_CAT", dbName);
                        row.put("TABLE_SCHEM", connSchema);
                        row.put("TABLE_NAME", collectionName);
                        row.put("COLUMN_NAME", columnName);
                        row.put("DATA_TYPE", getSqlDataType(columnTypeName));
                        row.put("TYPE_NAME", columnTypeName);
                        row.put("COLUMN_SIZE", -1);
                        row.put("BUFFER_LENGTH", -1);
                        row.put("DECIMAL_DIGITS", null);
                        row.put("NUM_PREC_RADIX", 10);
                        row.put("NULLABLE", DatabaseMetaData.columnNullableUnknown);
                        row.put("REMARKS", null);
                        row.put("COLUMN_DEF", null);
                        row.put("SQL_DATA_TYPE", Types.NULL);
                        row.put("SQL_DATETIME_SUB", Types.NULL);
                        row.put("CHAR_OCTET_LENGTH", null);
                        row.put("ORDINAL_POSITION", ordinalPosition);
                        row.put("IS_NULLABLE", ""); // empty string --- if the nullability for the column is unknown
                        row.put("SCOPE_CATALOG", null);
                        row.put("SCOPE_SCHEMA", null);
                        row.put("SCOPE_TABLE", null);
                        row.put("SOURCE_DATA_TYPE", null);
                        row.put("IS_AUTOINCREMENT", ""); //empty string --- if it cannot be determined whether the column is auto incremented
                        row.put("IS_GENERATEDCOLUMN", ""); // empty string --- if it cannot be determined whether this is a generated column
                        columns.add(row);
                };
            };
        });
        return new ArangoDbListResultSet<Object>(connection.createStatement(),columns);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        LOGGER.debug("getSchemas()");
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        LOGGER.debug("getSchemas(catalog={}, schemaPattern={})", new Object[]{catalog, schemaPattern});
        String dbName = catalog == null || catalog.isEmpty() ? connection.getCatalog() : catalog;
        String connSchema = connection.getSchema();

        List<Map<String, Object>> schemas = new ArrayList<>();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("TABLE_CATALOG", dbName);
        row.put("TABLE_SCHEM", connSchema);
        schemas.add(row);
        return new ArangoDbListResultSet<Object>(connection.createStatement(), schemas);
    }

    @Override
    public String getUserName() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        LOGGER.debug("getPrimaryKeys(catalog={}, schema={}, table={})", new Object[]{catalog, schema, table});
        String dbName = catalog == null || catalog.isEmpty() ? connection.getCatalog() : catalog;
        ArangoDatabase db = connection.getArangoDB().db(dbName);
        String connSchema = connection.getSchema();
        List<Map<String, Object>> primaryKeys = new ArrayList<>();

        db.getCollections().forEach(collection -> {
            if(collection.getIsSystem()) {
                return;
            }
            String collectionName = collection.getName();
            if(table != null && !collectionName.equals(table)) {
                return;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("TABLE_CAT", dbName);
            row.put("TABLE_SCHEM", connSchema);
            row.put("TABLE_NAME", collectionName);
            row.put("COLUMN_NAME", ArangoDbConstants.KEY_ATTRIBUTE);
            row.put("KEY_SEQ", 1);
            row.put("PK_NAME", String.format("%s_PrimaryKey", collectionName));
            primaryKeys.add(row);
        });
        return new ArangoDbListResultSet<Object>(connection.createStatement(), primaryKeys);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        LOGGER.debug("getImportedKeys(catalog={}, schema={}, table={})", new Object[]{catalog, schema, table});
        return new ArangoDbListResultSet<Object>(connection.createStatement(),new ArrayList<>());
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        LOGGER.debug("getExportedKeys(catalog={}, schema={}, table={})", new Object[]{catalog, schema, table});
        return new ArangoDbListResultSet<Object>(connection.createStatement(),new ArrayList<>());
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        LOGGER.debug("getCrossReference(parentCatalog={}, parentSchema={}, parentTable={}, foreignCatalog={}, foreignSchema={}, foreignTable={})", new Object[]{parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable});
        return new ArangoDbListResultSet<Object>(connection.createStatement(),new ArrayList<>());
    }




    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getUrl();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connection.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return true;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        ArangoDBVersion version = connection.getArangoDB().getVersion();
        return "ArangoDB " + version.getLicense();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        ArangoDBVersion version = connection.getArangoDB().getVersion();
        return version.getVersion();
    }

    @Override
    public String getDriverName() throws SQLException {
        return "ArangoDB JDBC Driver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "1.0.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 1;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "FOR,IN,RETURN,FILTER,SORT,LIMIT,LET,COLLECT,INSERT,UPDATE,REPLACE,REMOVE,UPSERT";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "database";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return null;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "database";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 256;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 2;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

   
    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return null;
    }
}
