package com.io.debezium.configserver.service.oracle;

import com.io.debezium.configserver.model.*;
import com.io.debezium.configserver.service.ConnectorIntegratorBase;
import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import com.io.debezium.configserver.model.*;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceConnector;
import org.jboss.logging.Logger;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

//@RegisterForReflection
public class OracleConnectorIntegrator extends ConnectorIntegratorBase {

    private static final Logger LOGGER = Logger.getLogger(OracleConnectorIntegrator.class);

    private static final Map<String, AdditionalPropertyMetadata> ORACLE_PROPERTIES;

    static {
        Map<String, AdditionalPropertyMetadata> additionalMetadata = new LinkedHashMap<>();

        // Connection properties
//        additionalMetadata.put(OracleConnectorConfig.HOSTNAME.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTION));
//        additionalMetadata.put(OracleConnectorConfig.PORT.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTION));
        additionalMetadata.put(OracleConnectorConfig.SERVER_NAME.name(), new AdditionalPropertyMetadata(true, ConnectorProperty.Category.CONNECTION));
        //additionalMetadata.put(OracleConnectorConfig.USER.name(), new AdditionalPropertyMetadata(true, ConnectorProperty.Category.CONNECTION));
        //additionalMetadata.put(OracleConnectorConfig.PASSWORD.name(), new AdditionalPropertyMetadata(true, ConnectorProperty.Category.CONNECTION));
//        additionalMetadata.put(OracleConnectorConfig.DATABASE_NAME.name(), new AdditionalPropertyMetadata(true, ConnectorProperty.Category.CONNECTION));
        //additionalMetadata.put(OracleConnectorConfig.URL.name(), new AdditionalPropertyMetadata(true, ConnectorProperty.Category.CONNECTION));
//        additionalMetadata.put(OracleConnectorConfig.PDB_NAME.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTION));
        additionalMetadata.put(OracleConnectorConfig.CONNECTOR_ADAPTER.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTION, enumArrayToList(OracleConnectorConfig.ConnectorAdapter.values()), true));
        //additionalMetadata.put(KafkaDatabaseHistory.BOOTSTRAP_SERVERS.name(), new AdditionalPropertyMetadata(true, ConnectorProperty.Category.CONNECTION));
        //additionalMetadata.put(KafkaDatabaseHistory.TOPIC.name(), new AdditionalPropertyMetadata(true, ConnectorProperty.Category.CONNECTION));

        // Filter properties
        additionalMetadata.put(OracleConnectorConfig.SCHEMA_INCLUDE_LIST.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.FILTERS));
        additionalMetadata.put(OracleConnectorConfig.SCHEMA_EXCLUDE_LIST.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.FILTERS));
        additionalMetadata.put(OracleConnectorConfig.TABLE_INCLUDE_LIST.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.FILTERS));
        additionalMetadata.put(OracleConnectorConfig.TABLE_EXCLUDE_LIST.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.FILTERS));
        additionalMetadata.put(OracleConnectorConfig.COLUMN_INCLUDE_LIST.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.FILTERS));
        additionalMetadata.put(OracleConnectorConfig.COLUMN_EXCLUDE_LIST.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.FILTERS));
        additionalMetadata.put(OracleConnectorConfig.TABLE_IGNORE_BUILTIN.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.FILTERS));

        // Snapshot properties
        additionalMetadata.put(OracleConnectorConfig.SNAPSHOT_MODE.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_SNAPSHOT, enumArrayToList(OracleConnectorConfig.SnapshotMode.values())));
        additionalMetadata.put(OracleConnectorConfig.SNAPSHOT_LOCKING_MODE.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_SNAPSHOT, enumArrayToList(OracleConnectorConfig.SnapshotLockingMode.values())));
        additionalMetadata.put(OracleConnectorConfig.SNAPSHOT_MODE_TABLES.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_SNAPSHOT, enumArrayToList(OracleConnectorConfig.SnapshotLockingMode.values())));
        additionalMetadata.put(OracleConnectorConfig.SNAPSHOT_FETCH_SIZE.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_SNAPSHOT));
        additionalMetadata.put(OracleConnectorConfig.SNAPSHOT_DELAY_MS.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_SNAPSHOT));
        additionalMetadata.put(OracleConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_SNAPSHOT));
        additionalMetadata.put(OracleConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR_SNAPSHOT));

        // LogMiner properties
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_STRATEGY.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_BATCH_SIZE_MIN.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_BATCH_SIZE_MAX.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_BATCH_SIZE_DEFAULT.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_MIN_MS.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_MAX_MS.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_DEFAULT_MS.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_INCREMENT_MS.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_VIEW_FETCH_SIZE.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_HOURS.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_MODE.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_ARCHIVE_DESTINATION_NAME.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOG_MINING_TRANSACTION_RETENTION.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));
        additionalMetadata.put(OracleConnectorConfig.LOB_ENABLED.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR_LOGMINER));

        // database history producer config
//        additionalMetadata.put(OracleConnectorConfig..name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTION_ADVANCED_REPLICATION);

        // Data type mapping properties:
        additionalMetadata.put(OracleConnectorConfig.TOMBSTONES_ON_DELETE.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR));
        //additionalMetadata.put(OracleConnectorConfig.DECIMAL_HANDLING_MODE.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR, enumArrayToList(OracleConnectorConfig.DecimalHandlingMode.values())));
        additionalMetadata.put(OracleConnectorConfig.TIME_PRECISION_MODE.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR, enumArrayToList(TemporalPrecisionMode.values())));
        additionalMetadata.put(OracleConnectorConfig.BINARY_HANDLING_MODE.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.CONNECTOR, enumArrayToList(OracleConnectorConfig.BinaryHandlingMode.values())));

        // Heartbeat properties
        additionalMetadata.put(Heartbeat.HEARTBEAT_INTERVAL.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.ADVANCED_HEARTBEAT));
        additionalMetadata.put(Heartbeat.HEARTBEAT_TOPICS_PREFIX.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.ADVANCED_HEARTBEAT));

        // Data type mapping properties - Advanced:
        // additional property added to UI Requirements document section for "Data type mapping properties"-advanced section:
        additionalMetadata.put(OracleConnectorConfig.CUSTOM_CONVERTERS.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR_ADVANCED));
        additionalMetadata.put(OracleConnectorConfig.TRUNCATE_COLUMN.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR_ADVANCED));
        additionalMetadata.put(OracleConnectorConfig.MASK_COLUMN.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR_ADVANCED));
        additionalMetadata.put(OracleConnectorConfig.MASK_COLUMN_WITH_HASH.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR_ADVANCED));
        additionalMetadata.put(OracleConnectorConfig.PROPAGATE_DATATYPE_SOURCE_TYPE.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR_ADVANCED));
        additionalMetadata.put(OracleConnectorConfig.PROPAGATE_COLUMN_SOURCE_TYPE.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR_ADVANCED));
        additionalMetadata.put(OracleConnectorConfig.MSG_KEY_COLUMNS.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR_ADVANCED));
        additionalMetadata.put(OracleConnectorConfig.PROVIDE_TRANSACTION_METADATA.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR_ADVANCED));
        additionalMetadata.put(OracleConnectorConfig.SANITIZE_FIELD_NAMES.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.CONNECTOR_ADVANCED));

        // Advanced configs (aka Runtime configs based on the PoC Requirements document
//        additionalMetadata.put(KafkaDatabaseHistory.RECOVERY_POLL_ATTEMPTS.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.ADVANCED));
//        additionalMetadata.put(KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.ADVANCED));
        additionalMetadata.put(OracleConnectorConfig.SKIPPED_OPERATIONS.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.ADVANCED));
        additionalMetadata.put(OracleConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.ADVANCED, enumArrayToList(OracleConnectorConfig.EventProcessingFailureHandlingMode.values())));
        additionalMetadata.put(OracleConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.ADVANCED, enumArrayToList(OracleConnectorConfig.EventProcessingFailureHandlingMode.values())));
        additionalMetadata.put(OracleConnectorConfig.MAX_QUEUE_SIZE.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.ADVANCED));
        additionalMetadata.put(OracleConnectorConfig.POLL_INTERVAL_MS.name(), new AdditionalPropertyMetadata(false, true, ConnectorProperty.Category.ADVANCED));
        additionalMetadata.put(OracleConnectorConfig.SOURCE_STRUCT_MAKER_VERSION.name(), new AdditionalPropertyMetadata(false, ConnectorProperty.Category.ADVANCED));

        ORACLE_PROPERTIES = Collections.unmodifiableMap(additionalMetadata);
    }

    @Override
    public FilterValidationResult validateFilters(Map<String, String> properties) {
        PropertiesValidationResult result = validateProperties(properties);
        if (result.status == PropertiesValidationResult.Status.INVALID) {
            return FilterValidationResult.invalid(result.propertyValidationResults);
        }

        OracleConnectorConfig config = new OracleConnectorConfig(Configuration.from(properties));
        OracleConnection connection = new OracleConnection(config.getJdbcConfig(), null);
        Set<TableId> tables;
        try {
            tables = connection.readTableNames(config.getCatalogName(), null ,null , new String[]{ "TABLE", "VIEW" });
            List<DataCollection> matchingTables = tables.stream()
                    .filter(tableId -> config.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                    .map(tableId -> new DataCollection(tableId.schema(), tableId.table()))
                    .collect(Collectors.toList());

            return FilterValidationResult.valid(matchingTables);
        }
        catch (SQLException e) {
            LOGGER.error("validate config exception: ", e);
            throw new DebeziumException(e);
        }
    }

    @Override
    protected ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("oracle", "Oracle","source", true);
    }

    @Override
    protected SourceConnector getConnector() {
        return new OracleConnector();
    }

    @Override
    public Map<String, AdditionalPropertyMetadata> allPropertiesWithAdditionalMetadata() {
        return ORACLE_PROPERTIES;
    }

    @Override
    public Field.Set getAllConnectorFields() {
        return OracleConnectorConfig.ALL_FIELDS;
    }
}
