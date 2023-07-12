package com.netapp.spark;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.IntStream.range;
import static org.apache.arrow.adapter.jdbc.JdbcToArrow.sqlToArrowVectorIterator;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowUtils.jdbcToArrowSchema;
import static org.apache.arrow.util.Preconditions.checkState;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.CancelResult;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.util.ArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class SparkSQLProducer implements FlightSqlProducer {
    static Logger logger = LoggerFactory.getLogger(SparkSQLProducer.class);
    private static final String DATABASE_URI = "jdbc:hive2://localhost:10000";
    private static final String METASTORE_URI = "jdbc:derby:;databaseName=metastore_db;create=true";
    SparkSession sparkSession;
    static Location location = Location.forGrpcInsecure("localhost", 33333);
    Map<String, Dataset<Row>> datasets = new ConcurrentHashMap<>();
    private final BufferAllocator rootAllocator = new RootAllocator();
    private SqlInfoBuilder sqlInfoBuilder;
    private static final Calendar DEFAULT_CALENDAR = JdbcToArrowUtils.getUtcCalendar();
    private final Cache<ByteString, StatementContext<PreparedStatement>> preparedStatementLoadingCache;
    private final Cache<ByteString, StatementContext<Statement>> statementLoadingCache;
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    private static class StatementRemovalListener<T extends Statement>
            implements RemovalListener<ByteString, StatementContext<T>> {
        @Override
        public void onRemoval(final RemovalNotification<ByteString, StatementContext<T>> notification) {
            try {
                AutoCloseables.close(notification.getValue());
            } catch (final Exception e) {
                // swallow
            }
        }
    }

    public SparkSQLProducer(SparkSession sparkSession) throws SQLException {
        this.sparkSession = sparkSession;

        preparedStatementLoadingCache =
                CacheBuilder.newBuilder()
                        .maximumSize(100)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
                        .removalListener(new StatementRemovalListener<PreparedStatement>())
                        .build();

        statementLoadingCache =
                CacheBuilder.newBuilder()
                        .maximumSize(100)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
                        .removalListener(new StatementRemovalListener<>())
                        .build();
    }

    public SqlInfoBuilder initSqlInfoBuilder(CallContext context) {
        if (sqlInfoBuilder == null) {
            sqlInfoBuilder = new SqlInfoBuilder();
            var constr = getConnection(context);
            try (var connection = DriverManager.getConnection(constr)) {
                var metaData = connection.getMetaData();
                sqlInfoBuilder = sqlInfoBuilder.withFlightSqlServerName(metaData.getDatabaseProductName())
                        .withFlightSqlServerVersion(metaData.getDatabaseProductVersion())
                        .withFlightSqlServerArrowVersion(metaData.getDriverVersion())
                        .withFlightSqlServerReadOnly(metaData.isReadOnly())
                        .withFlightSqlServerSql(true)
                        .withFlightSqlServerSubstrait(false)
                        .withFlightSqlServerTransaction(FlightSql.SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
                        .withSqlIdentifierQuoteChar(metaData.getIdentifierQuoteString())
                        .withSqlDdlCatalog(metaData.supportsCatalogsInDataManipulation())
                        .withSqlDdlSchema(metaData.supportsSchemasInDataManipulation())
                        .withSqlDdlTable(metaData.allTablesAreSelectable())
                        .withSqlIdentifierCase(metaData.storesMixedCaseIdentifiers() ?
                                FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE :
                                metaData.storesUpperCaseIdentifiers() ?
                                        FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UPPERCASE :
                                        metaData.storesLowerCaseIdentifiers() ?
                                                FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE :
                                                FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
                        .withSqlQuotedIdentifierCase(metaData.storesMixedCaseQuotedIdentifiers() ?
                                FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE :
                                metaData.storesUpperCaseQuotedIdentifiers() ?
                                        FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UPPERCASE :
                                        metaData.storesLowerCaseQuotedIdentifiers() ?
                                                FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE :
                                                FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return sqlInfoBuilder;
    }

    public static void main(String[] args) {
        init3(args);
    }

    public static void init3(String[] args) {
        var sql = "SELECT * FROM global_temp.spark_connect_info";
        /*var allocator = new RootAllocator();
        try (var client = new FlightSqlClient(FlightClient.builder(allocator, location).build())) {
            var preparedStatement = client.prepare(sql);
            var flightInfo = preparedStatement.execute();
            for (var endpoint : flightInfo.getEndpoints()) {
                //client.get
                // 2. Get a stream of results as Arrow vectors
                try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                    // 3. Iterate through the stream until the end
                    while (stream.next()) {
                        // 4. Get a chunk of results (VectorSchemaRoot) and print it to the console
                        VectorSchemaRoot vectorSchemaRoot = stream.getRoot();
                        System.out.println(vectorSchemaRoot.contentToTSVString());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error getting spark connect info", e);
        }*/

        try (var connection = DriverManager.getConnection("jdbc:arrow-flight-sql://localhost:33333/?useEncryption=false");
             var statement = connection.prepareStatement(sql);
             var resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                var type = resultSet.getString(1);
                var langport = resultSet.getInt(2)+10;
                var secret = resultSet.getString(3);
                System.out.println(type + " " + langport + " " + secret);
            }
        } catch (Exception e) {
            logger.error("Error getting spark connect info", e);
        }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        return FlightSqlProducer.super.getFlightInfo(context, descriptor);
    }

    @Override
    public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
        return FlightSqlProducer.super.getSchema(context, descriptor);
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        FlightSqlProducer.super.getStream(context, ticket, listener);
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {

    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return FlightSqlProducer.super.acceptPut(context, flightStream, ackStream);
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
        FlightSqlProducer.super.doExchange(context, reader, writer);
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        FlightSqlProducer.super.listActions(context, listener);
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        FlightSqlProducer.super.doAction(context, action, listener);
    }

    @Override
    public void beginSavepoint(FlightSql.ActionBeginSavepointRequest request, CallContext context, StreamListener<FlightSql.ActionBeginSavepointResult> listener) {
        FlightSqlProducer.super.beginSavepoint(request, context, listener);
    }

    @Override
    public void beginTransaction(FlightSql.ActionBeginTransactionRequest request, CallContext context, StreamListener<FlightSql.ActionBeginTransactionResult> listener) {
        FlightSqlProducer.super.beginTransaction(request, context, listener);
    }

    @Override
    public void cancelQuery(FlightInfo info, CallContext context, StreamListener<CancelResult> listener) {
        FlightSqlProducer.super.cancelQuery(info, context, listener);
    }

    private static ByteBuffer serializeMetadata(final Schema schema) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);

            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize schema", e);
        }
    }

    @Override
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        var constr = getConnection(context);
        executorService.submit(() -> {
            if (constr.contains("hive")) {
                var preparedStatementHandle = copyFrom(randomUUID().toString().getBytes(UTF_8));
                var df = sparkSession.sql(request.getQuery());
                var sparkSchema = df.schema();
                datasets.put(preparedStatementHandle.toStringUtf8(), df);
                var timeZoneId = DateTimeUtils.TimeZoneUTC().getID();
                var arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, timeZoneId);
                var serializedSchema = serializeMetadata(arrowSchema);
                var byteString = ByteString.copyFrom(serializedSchema);
                var req = FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                        .setDatasetSchema(byteString)
                        .setPreparedStatementHandle(preparedStatementHandle)
                        .build();
                var res = new Result(pack(req).toByteArray());

                final StatementContext<PreparedStatement> preparedStatementContext =
                        new StatementContext<>(request.getQuery());
                preparedStatementLoadingCache.put(preparedStatementHandle, preparedStatementContext);
                listener.onNext(res);
                listener.onCompleted();
            } else {
                try {
                    final ByteString preparedStatementHandle = copyFrom(randomUUID().toString().getBytes(UTF_8));
                    // Ownership of the connection will be passed to the context. Do NOT close!
                    final Connection connection = DriverManager.getConnection(constr);
                    final PreparedStatement preparedStatement = connection.prepareStatement(request.getQuery(),
                            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    final StatementContext<PreparedStatement> preparedStatementContext =
                            new StatementContext<>(preparedStatement, request.getQuery());

                    preparedStatementLoadingCache.put(preparedStatementHandle, preparedStatementContext);

                    final Schema parameterSchema =
                            jdbcToArrowSchema(preparedStatement.getParameterMetaData(), DEFAULT_CALENDAR);

                    final ResultSetMetaData metaData = preparedStatement.getMetaData();
                    final ByteString bytes = isNull(metaData) ?
                            ByteString.EMPTY :
                            ByteString.copyFrom(
                                    serializeMetadata(jdbcToArrowSchema(metaData, DEFAULT_CALENDAR)));
                    final FlightSql.ActionCreatePreparedStatementResult result = FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                            .setDatasetSchema(bytes)
                            .setParameterSchema(copyFrom(serializeMetadata(parameterSchema)))
                            .setPreparedStatementHandle(preparedStatementHandle)
                            .build();
                    listener.onNext(new Result(pack(result).toByteArray()));
                } catch (final SQLException e) {
                    listener.onError(CallStatus.INTERNAL
                            .withDescription("Failed to create prepared statement: " + e)
                            .toRuntimeException());
                    return;
                } catch (final Throwable t) {
                    listener.onError(CallStatus.INTERNAL.withDescription("Unknown error: " + t).toRuntimeException());
                    return;
                }
                listener.onCompleted();
            }
        });

        /*var query = request.getQuery();
        var df = sparkSession.sql(query);
        df.toLocalIterator().forEachRemaining(row -> {
            var result = new Result("hello".getBytes());
            listener.onNext(result);
        });
        listener.onCompleted();*/
    }

    @Override
    public void createPreparedSubstraitPlan(FlightSql.ActionCreatePreparedSubstraitPlanRequest request, CallContext context, StreamListener<FlightSql.ActionCreatePreparedStatementResult> listener) {
        FlightSqlProducer.super.createPreparedSubstraitPlan(request, context, listener);
    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        executorService.submit(() -> {
            try {
                preparedStatementLoadingCache.invalidate(request.getPreparedStatementHandle());
            } catch (final Exception e) {
                listener.onError(e);
                return;
            }
            listener.onCompleted();
        });
    }

    @Override
    public void endSavepoint(FlightSql.ActionEndSavepointRequest request, CallContext context, StreamListener<Result> listener) {
        FlightSqlProducer.super.endSavepoint(request, context, listener);
    }

    @Override
    public void endTransaction(FlightSql.ActionEndTransactionRequest request, CallContext context, StreamListener<Result> listener) {
        FlightSqlProducer.super.endTransaction(request, context, listener);
    }

    @Override
    public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        var constr = getConnection(context);
        ByteString handle = copyFrom(randomUUID().toString().getBytes(UTF_8));
        if (constr.contains("hive")) {
            var df = sparkSession.sql(command.getQuery());
            datasets.put(handle.toStringUtf8(), df);
            var timeZoneId = DateTimeUtils.TimeZoneUTC().getID();
            var schema = ArrowUtils.toArrowSchema(df.schema(), timeZoneId);
            //var paths = descriptor.getPath();
            //var bytes = "sql".getBytes(); //paths.get(0).getBytes(StandardCharsets.UTF_8);
            /*FlightEndpoint flightEndpoint = new FlightEndpoint(
                    new Ticket(descriptor.getCommand()), location);
            return new FlightInfo(
                    schema,
                    descriptor,
                    Collections.singletonList(flightEndpoint),
                    /*bytes=*-1,
                    df.count()
            );*/
            FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                    .setStatementHandle(handle)
                    .build();
            return getFlightInfoForSchema(ticket, descriptor, schema);
        } else {
            try {
                // Ownership of the connection will be passed to the context. Do NOT close!
                final Connection connection = DriverManager.getConnection(constr);
                final Statement statement = connection.createStatement(
                        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                final String query = command.getQuery();
                final StatementContext<Statement> statementContext = new StatementContext<>(statement, query);

                statementLoadingCache.put(handle, statementContext);
                final ResultSet resultSet = statement.executeQuery(query);

                FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                        .setStatementHandle(handle)
                        .build();
                return getFlightInfoForSchema(ticket, descriptor,
                        jdbcToArrowSchema(resultSet.getMetaData(), DEFAULT_CALENDAR));
            } catch (final SQLException e) {
                logger.error(
                        format("There was a problem executing the prepared statement: <%s>.", e.getMessage()),
                        e);
                throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
            }
        }
        //return new FlightInfo(new FlightDescriptor("spark", "spark_connect_info"), new Schema(new ArrayList<>(), new ArrayList<>()), new ArrayList<>(), -1, -1, -1, -1);
    }

    @Override
    public FlightInfo getFlightInfoSubstraitPlan(FlightSql.CommandStatementSubstraitPlan command, CallContext context, FlightDescriptor descriptor) {
        return FlightSqlProducer.super.getFlightInfoSubstraitPlan(command, context, descriptor);
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        var constr = getConnection(context);
        if (constr.contains("hive")) {
            var handle = command.getPreparedStatementHandle().toStringUtf8();
            var df = datasets.get(handle);
            var structType = df.schema();
            //var count = df.count();
            var timeZoneId = DateTimeUtils.TimeZoneUTC().getID();
            var schema = ArrowUtils.toArrowSchema(structType, timeZoneId);
            //var paths = descriptor.getPath();
            //var bytes = "sql".getBytes(); //paths.get(0).getBytes(StandardCharsets.UTF_8);
            /*FlightEndpoint flightEndpoint = new FlightEndpoint(
                    new Ticket(descriptor.getCommand()), location);
            return new FlightInfo(
                    schema,
                    descriptor,
                    Collections.singletonList(flightEndpoint),
                    /*bytes=*-1,
                    count
            );*/
            return getFlightInfoForSchema(command, descriptor, schema);
        } else {
            final ByteString preparedStatementHandle = command.getPreparedStatementHandle();
            StatementContext<PreparedStatement> statementContext =
                    preparedStatementLoadingCache.getIfPresent(preparedStatementHandle);
            try {
                assert statementContext != null;
                PreparedStatement statement = statementContext.getStatement();

                ResultSetMetaData metaData = statement.getMetaData();
                return getFlightInfoForSchema(command, descriptor,
                        jdbcToArrowSchema(metaData, DEFAULT_CALENDAR));
            } catch (final SQLException e) {
                logger.error(
                        format("There was a problem executing the prepared statement: <%s>.", e.getMessage()),
                        e);
                throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
            }
        }
    }

    @Override
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public SchemaResult getSchemaPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        return FlightSqlProducer.super.getSchemaPreparedStatement(command, context, descriptor);
    }

    @Override
    public SchemaResult getSchemaSubstraitPlan(FlightSql.CommandStatementSubstraitPlan command, CallContext context, FlightDescriptor descriptor) {
        return FlightSqlProducer.super.getSchemaSubstraitPlan(command, context, descriptor);
    }

    @Override
    public void getStreamStatement(FlightSql.TicketStatementQuery ticketStatementQuery, CallContext context, ServerStreamListener listener) {
        var constr = getConnection(context);
        final ByteString handle = ticketStatementQuery.getStatementHandle();
        if (constr.contains("hive")) {
            var handleByteString = handle.toStringUtf8();
            var df = datasets.get(handleByteString);
            var timeZoneId = DateTimeUtils.TimeZoneUTC().getID();
            var schema = ArrowUtils.toArrowSchema(df.schema(), timeZoneId);
            try (final VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
                final VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                listener.start(vectorSchemaRoot);
                var it = JavaConverters.asJavaIterator(df.toArrowBatchRdd().toLocalIterator());
                while (it.hasNext()) {
                    var arrowBatchBytes = it.next();
                    var in = new ByteArrayInputStream(arrowBatchBytes);
                    var arrowRecordBatch = MessageSerializer.deserializeRecordBatch(new ReadChannel(Channels.newChannel(in)), rootAllocator);
                    loader.load(arrowRecordBatch);
                    listener.putNext();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                listener.completed();
            }
        } else {
            final StatementContext<Statement> statementContext =
                    Objects.requireNonNull(statementLoadingCache.getIfPresent(handle));
            try (final ResultSet resultSet = statementContext.getStatement().getResultSet()) {
                final Schema schema = jdbcToArrowSchema(resultSet.getMetaData(), DEFAULT_CALENDAR);
                try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
                    final VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                    listener.start(vectorSchemaRoot);

                    final ArrowVectorIterator iterator = sqlToArrowVectorIterator(resultSet, rootAllocator);
                    while (iterator.hasNext()) {
                        final VectorUnloader unloader = new VectorUnloader(iterator.next());
                        loader.load(unloader.getRecordBatch());
                        listener.putNext();
                        vectorSchemaRoot.clear();
                    }

                    listener.putNext();
                }
            } catch (SQLException | IOException e) {
                logger.error(format("Failed to getStreamPreparedStatement: <%s>.", e.getMessage()), e);
                listener.error(e);
            } finally {
                listener.completed();
                statementLoadingCache.invalidate(handle);
            }
        }
    }

    @Override
    public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context, ServerStreamListener listener) {
        var constr = getConnection(context);
        final ByteString handle = command.getPreparedStatementHandle();
        if (constr.contains("hive")) {
            var handleByteString = handle.toStringUtf8();
            var df = datasets.get(handleByteString);
            var timeZoneId = DateTimeUtils.TimeZoneUTC().getID();
            var schema = ArrowUtils.toArrowSchema(df.schema(), timeZoneId);
            try (final VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
                final VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                listener.start(vectorSchemaRoot);
                var it = JavaConverters.asJavaIterator(df.toArrowBatchRdd().toLocalIterator());
                while (it.hasNext()) {
                    var arrowBatchBytes = it.next();
                    var in = new ByteArrayInputStream(arrowBatchBytes);
                    var arrowRecordBatch = MessageSerializer.deserializeRecordBatch(new ReadChannel(Channels.newChannel(in)), rootAllocator);
                    loader.load(arrowRecordBatch);
                    listener.putNext();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                listener.completed();
            }
        } else {
            StatementContext<PreparedStatement> statementContext = preparedStatementLoadingCache.getIfPresent(handle);
            Objects.requireNonNull(statementContext);
            final PreparedStatement statement = statementContext.getStatement();
            try (final ResultSet resultSet = statement.executeQuery()) {
                final Schema schema = jdbcToArrowSchema(resultSet.getMetaData(), DEFAULT_CALENDAR);
                try (final VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
                    final VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                    listener.start(vectorSchemaRoot);

                    final ArrowVectorIterator iterator = sqlToArrowVectorIterator(resultSet, rootAllocator);
                    while (iterator.hasNext()) {
                        final VectorSchemaRoot batch = iterator.next();
                        if (batch.getRowCount() == 0) {
                            break;
                        }
                        final VectorUnloader unloader = new VectorUnloader(batch);
                        loader.load(unloader.getRecordBatch());
                        listener.putNext();
                        vectorSchemaRoot.clear();
                    }

                    listener.putNext();
                }
            } catch (final SQLException | IOException e) {
                logger.error(format("Failed to getStreamPreparedStatement: <%s>.", e.getMessage()), e);
                listener.error(CallStatus.INTERNAL.withDescription("Failed to prepare statement: " + e).toRuntimeException());
            } finally {
                listener.completed();
            }
        }
    }

    @Override
    public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate command, CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return null;
    }

    @Override
    public Runnable acceptPutSubstraitPlan(FlightSql.CommandStatementSubstraitPlan command, CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return FlightSqlProducer.super.acceptPutSubstraitPlan(command, context, flightStream, ackStream);
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate command, CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return null;
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery command, CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return null;
    }

    private <T extends Message> FlightInfo getFlightInfoForSchema(final T request, final FlightDescriptor descriptor,
                                                                  final Schema schema) {
        final Ticket ticket = new Ticket(pack(request).toByteArray());
        // TODO Support multiple endpoints.
        final List<FlightEndpoint> endpoints = singletonList(new FlightEndpoint(ticket, location));

        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SQL_INFO_SCHEMA);
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command, CallContext context, ServerStreamListener listener) {
        initSqlInfoBuilder(context).send(command.getInfoList(), listener);
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_TYPE_INFO_SCHEMA);
    }

    private static VectorSchemaRoot getTypeInfoRoot(FlightSql.CommandGetXdbcTypeInfo request, ResultSet typeInfo,
                                                    final BufferAllocator allocator)
            throws SQLException {
        Preconditions.checkNotNull(allocator, "BufferAllocator cannot be null.");

        VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TYPE_INFO_SCHEMA, allocator);

        Map<FieldVector, String> mapper = new HashMap<>();
        mapper.put(root.getVector("type_name"), "TYPE_NAME");
        mapper.put(root.getVector("data_type"), "DATA_TYPE");
        mapper.put(root.getVector("column_size"), "PRECISION");
        mapper.put(root.getVector("literal_prefix"), "LITERAL_PREFIX");
        mapper.put(root.getVector("literal_suffix"), "LITERAL_SUFFIX");
        mapper.put(root.getVector("create_params"), "CREATE_PARAMS");
        mapper.put(root.getVector("nullable"), "NULLABLE");
        mapper.put(root.getVector("case_sensitive"), "CASE_SENSITIVE");
        mapper.put(root.getVector("searchable"), "SEARCHABLE");
        mapper.put(root.getVector("unsigned_attribute"), "UNSIGNED_ATTRIBUTE");
        mapper.put(root.getVector("fixed_prec_scale"), "FIXED_PREC_SCALE");
        mapper.put(root.getVector("auto_increment"), "AUTO_INCREMENT");
        mapper.put(root.getVector("local_type_name"), "LOCAL_TYPE_NAME");
        mapper.put(root.getVector("minimum_scale"), "MINIMUM_SCALE");
        mapper.put(root.getVector("maximum_scale"), "MAXIMUM_SCALE");
        mapper.put(root.getVector("sql_data_type"), "SQL_DATA_TYPE");
        mapper.put(root.getVector("datetime_subcode"), "SQL_DATETIME_SUB");
        mapper.put(root.getVector("num_prec_radix"), "NUM_PREC_RADIX");

        Predicate<ResultSet> predicate;
        if (request.hasDataType()) {
            predicate = (resultSet) -> {
                try {
                    return resultSet.getInt("DATA_TYPE") == request.getDataType();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            };
        } else {
            predicate = (resultSet -> true);
        }

        int rows = saveToVectors(mapper, typeInfo, true, predicate);

        root.setRowCount(rows);
        return root;
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context, ServerStreamListener listener) {
        var constr = getConnection(context);
        try (final Connection connection = DriverManager.getConnection(constr);
             final ResultSet typeInfo = connection.getMetaData().getTypeInfo();
             final VectorSchemaRoot vectorSchemaRoot = getTypeInfoRoot(request, typeInfo, rootAllocator)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (SQLException e) {
            logger.error(format("Failed to getStreamCatalogs: <%s>.", e.getMessage()), e);
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_CATALOGS_SCHEMA);
    }

    String getConnection(CallContext context) {
        ServerHeaderMiddleware serverHeaderMiddleware = (ServerHeaderMiddleware) context.getMiddleware().get(FlightConstants.HEADER_KEY);
        var mstr = serverHeaderMiddleware.headers().get("database");
        return mstr != null && mstr.equals("metastore_db") ? METASTORE_URI : DATABASE_URI;
    }

    @Override
    public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
        var constr = getConnection(context);
        try (final Connection connection = DriverManager.getConnection(constr);
             final ResultSet catalogs = connection.getMetaData().getCatalogs();
             final VectorSchemaRoot vectorSchemaRoot = getCatalogsRoot(catalogs, rootAllocator)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (SQLException e) {
            logger.error(format("Failed to getStreamCatalogs: <%s>.", e.getMessage()), e);
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
    }

    private static VectorSchemaRoot getSchemasRoot(final ResultSet data, final BufferAllocator allocator)
            throws SQLException {
        final VarCharVector catalogs = new VarCharVector("catalog_name", allocator);
        final VarCharVector schemas =
                new VarCharVector("db_schema_name", FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);
        final List<FieldVector> vectors = ImmutableList.of(catalogs, schemas);
        vectors.forEach(FieldVector::allocateNew);
        final Map<FieldVector, String> vectorToColumnName = ImmutableMap.of(
                catalogs, "TABLE_CATALOG",
                schemas, "TABLE_SCHEM");
        saveToVectors(vectorToColumnName, data);
        final int rows = vectors.stream().map(FieldVector::getValueCount).findAny().orElseThrow(IllegalStateException::new);
        vectors.forEach(vector -> vector.setValueCount(rows));
        return new VectorSchemaRoot(vectors);
    }

    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas command, CallContext context, ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern = command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
        try (final Connection connection = DriverManager.getConnection(getConnection(context));
             final ResultSet schemas = connection.getMetaData().getSchemas(catalog, schemaFilterPattern);
             final VectorSchemaRoot vectorSchemaRoot = getSchemasRoot(schemas, rootAllocator)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (SQLException e) {
            logger.error(format("Failed to getStreamSchemas: <%s>.", e.getMessage()), e);
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables request, CallContext context, FlightDescriptor descriptor) {
        Schema schemaToUse = Schemas.GET_TABLES_SCHEMA;
        if (!request.getIncludeSchema()) {
            schemaToUse = Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        }
        return getFlightInfoForSchema(request, descriptor, schemaToUse);
    }

    private static <T extends FieldVector> int saveToVectors(final Map<T, String> vectorToColumnName,
                                                             final ResultSet data, boolean emptyToNull)
            throws SQLException {
        Predicate<ResultSet> alwaysTrue = (resultSet) -> true;
        return saveToVectors(vectorToColumnName, data, emptyToNull, alwaysTrue);
    }

    private static <T extends FieldVector> int saveToVectors(final Map<T, String> vectorToColumnName,
                                                             final ResultSet data, boolean emptyToNull,
                                                             Predicate<ResultSet> resultSetPredicate)
            throws SQLException {
        Objects.requireNonNull(vectorToColumnName, "vectorToColumnName cannot be null.");
        Objects.requireNonNull(data, "data cannot be null.");
        final Set<Map.Entry<T, String>> entrySet = vectorToColumnName.entrySet();
        int rows = 0;

        while (data.next()) {
            if (!resultSetPredicate.test(data)) {
                continue;
            }
            for (final Map.Entry<T, String> vectorToColumn : entrySet) {
                final T vector = vectorToColumn.getKey();
                final String columnName = vectorToColumn.getValue();
                if (vector instanceof VarCharVector) {
                    String thisData = data.getString(columnName);
                    saveToVector(emptyToNull ? emptyToNull(thisData) : thisData, (VarCharVector) vector, rows);
                } else if (vector instanceof IntVector) {
                    final int intValue = data.getInt(columnName);
                    saveToVector(data.wasNull() ? null : intValue, (IntVector) vector, rows);
                } else if (vector instanceof UInt1Vector) {
                    final byte byteValue = data.getByte(columnName);
                    saveToVector(data.wasNull() ? null : byteValue, (UInt1Vector) vector, rows);
                } else if (vector instanceof BitVector) {
                    final byte byteValue = data.getByte(columnName);
                    saveToVector(data.wasNull() ? null : byteValue, (BitVector) vector, rows);
                } else if (vector instanceof ListVector) {
                    String createParamsValues = data.getString(columnName);

                    UnionListWriter writer = ((ListVector) vector).getWriter();

                    BufferAllocator allocator = vector.getAllocator();
                    final ArrowBuf buf = allocator.buffer(1024);

                    writer.setPosition(rows);
                    writer.startList();

                    if (createParamsValues != null) {
                        String[] split = createParamsValues.split(",");

                        range(0, split.length)
                                .forEach(i -> {
                                    byte[] bytes = split[i].getBytes(UTF_8);
                                    Preconditions.checkState(bytes.length < 1024,
                                            "The amount of bytes is greater than what the ArrowBuf supports");
                                    buf.setBytes(0, bytes);
                                    writer.varChar().writeVarChar(0, bytes.length, buf);
                                });
                    }
                    buf.close();
                    writer.endList();
                } else {
                    throw CallStatus.INVALID_ARGUMENT.withDescription("Provided vector not supported").toRuntimeException();
                }
            }
            rows ++;
        }
        for (final Map.Entry<T, String> vectorToColumn : entrySet) {
            vectorToColumn.getKey().setValueCount(rows);
        }

        return rows;
    }

    private static <T extends FieldVector> void saveToVectors(final Map<T, String> vectorToColumnName,
                                                              final ResultSet data)
            throws SQLException {
        saveToVectors(vectorToColumnName, data, false);
    }

    private static void saveToVector(final Byte data, final UInt1Vector vector, final int index) {
        vectorConsumer(
                data,
                vector,
                fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, theData));
    }

    private static void saveToVector(final Byte data, final BitVector vector, final int index) {
        vectorConsumer(
                data,
                vector,
                fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, theData));
    }

    private static void saveToVector(final String data, final VarCharVector vector, final int index) {
        preconditionCheckSaveToVector(vector, index);
        vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, new Text(theData)));
    }

    private static void saveToVector(final Integer data, final IntVector vector, final int index) {
        preconditionCheckSaveToVector(vector, index);
        vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, theData));
    }

    private static void saveToVector(final byte[] data, final VarBinaryVector vector, final int index) {
        preconditionCheckSaveToVector(vector, index);
        vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, theData));
    }

    private static void preconditionCheckSaveToVector(final FieldVector vector, final int index) {
        Objects.requireNonNull(vector, "vector cannot be null.");
        checkState(index >= 0, "Index must be a positive number!");
    }

    private static <T, V extends FieldVector> void vectorConsumer(final T data, final V vector,
                                                                  final Consumer<V> consumerIfNullable,
                                                                  final BiConsumer<T, V> defaultConsumer) {
        if (isNull(data)) {
            consumerIfNullable.accept(vector);
            return;
        }
        defaultConsumer.accept(data, vector);
    }

    private static ArrowType getArrowTypeFromJdbcType(final int jdbcDataType, final int precision, final int scale) {
        final ArrowType type =
                JdbcToArrowUtils.getArrowTypeFromJdbcType(new JdbcFieldInfo(jdbcDataType, precision, scale), DEFAULT_CALENDAR);
        return isNull(type) ? ArrowType.Utf8.INSTANCE : type;
    }

    private static VectorSchemaRoot getTablesRoot(final DatabaseMetaData databaseMetaData,
                                                  final BufferAllocator allocator,
                                                  final boolean includeSchema,
                                                  final String catalog,
                                                  final String schemaFilterPattern,
                                                  final String tableFilterPattern,
                                                  final String... tableTypes)
            throws SQLException, IOException {
        /*
         * TODO Fix DerbyDB inconsistency if possible.
         * During the early development of this prototype, an inconsistency has been found in the database
         * used for this demonstration; as DerbyDB does not operate with the concept of catalogs, fetching
         * the catalog name for a given table from `DatabaseMetadata#getColumns` and `DatabaseMetadata#getSchemas`
         * returns null, as expected. However, the inconsistency lies in the fact that accessing the same
         * information -- that is, the catalog name for a given table -- from `DatabaseMetadata#getSchemas`
         * returns an empty String.The temporary workaround for this was making sure we convert the empty Strings
         * to null using `com.google.common.base.Strings#emptyToNull`.
         */
        Objects.requireNonNull(allocator, "BufferAllocator cannot be null.");
        final VarCharVector catalogNameVector = new VarCharVector("catalog_name", allocator);
        final VarCharVector schemaNameVector = new VarCharVector("db_schema_name", allocator);
        final VarCharVector tableNameVector =
                new VarCharVector("table_name", FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);
        final VarCharVector tableTypeVector =
                new VarCharVector("table_type", FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);

        final List<FieldVector> vectors = new ArrayList<>(4);
        vectors.add(catalogNameVector);
        vectors.add(schemaNameVector);
        vectors.add(tableNameVector);
        vectors.add(tableTypeVector);

        vectors.forEach(FieldVector::allocateNew);

        final Map<FieldVector, String> vectorToColumnName = ImmutableMap.of(
                catalogNameVector, "TABLE_CAT",
                schemaNameVector, "TABLE_SCHEM",
                tableNameVector, "TABLE_NAME",
                tableTypeVector, "TABLE_TYPE");

        try (final ResultSet data =
                     Objects.requireNonNull(
                                     databaseMetaData,
                                     format("%s cannot be null.", databaseMetaData.getClass().getName()))
                             .getTables(catalog, schemaFilterPattern, tableFilterPattern, tableTypes)) {

            saveToVectors(vectorToColumnName, data, true);
            final int rows =
                    vectors.stream().map(FieldVector::getValueCount).findAny().orElseThrow(IllegalStateException::new);
            vectors.forEach(vector -> vector.setValueCount(rows));

            if (includeSchema) {
                final VarBinaryVector tableSchemaVector =
                        new VarBinaryVector("table_schema", FieldType.notNullable(Types.MinorType.VARBINARY.getType()), allocator);
                tableSchemaVector.allocateNew(rows);

                try (final ResultSet columnsData =
                             databaseMetaData.getColumns(catalog, schemaFilterPattern, tableFilterPattern, null)) {
                    final Map<String, List<Field>> tableToFields = new HashMap<>();

                    while (columnsData.next()) {
                        final String catalogName = columnsData.getString("TABLE_CAT");
                        final String schemaName = columnsData.getString("TABLE_SCHEM");
                        final String tableName = columnsData.getString("TABLE_NAME");
                        final String typeName = columnsData.getString("TYPE_NAME");
                        final String fieldName = columnsData.getString("COLUMN_NAME");
                        final int dataType = columnsData.getInt("DATA_TYPE");
                        final boolean isNullable = columnsData.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls;
                        final int precision = columnsData.getInt("COLUMN_SIZE");
                        final int scale = columnsData.getInt("DECIMAL_DIGITS");
                        boolean isAutoIncrement = false;
                                //Objects.equals(columnsData.getString("IS_AUTOINCREMENT"), "YES");

                        final List<Field> fields = tableToFields.computeIfAbsent(tableName, tableName_ -> new ArrayList<>());

                        final FlightSqlColumnMetadata columnMetadata = new FlightSqlColumnMetadata.Builder()
                                .catalogName(catalogName)
                                .schemaName(schemaName)
                                .tableName(tableName)
                                .typeName(typeName)
                                .precision(precision)
                                .scale(scale)
                                .isAutoIncrement(isAutoIncrement)
                                .build();

                        final Field field =
                                new Field(
                                        fieldName,
                                        new FieldType(
                                                isNullable,
                                                getArrowTypeFromJdbcType(dataType, precision, scale),
                                                null,
                                                columnMetadata.getMetadataMap()),
                                        null);
                        fields.add(field);
                    }

                    for (int index = 0; index < rows; index++) {
                        final String tableName = tableNameVector.getObject(index).toString();
                        final Schema schema = new Schema(tableToFields.get(tableName));
                        saveToVector(
                                copyFrom(serializeMetadata(schema)).toByteArray(),
                                tableSchemaVector, index);
                    }
                }

                tableSchemaVector.setValueCount(rows);
                vectors.add(tableSchemaVector);
            }
        }

        return new VectorSchemaRoot(vectors);
    }

    @Override
    public void getStreamTables(FlightSql.CommandGetTables command, CallContext context, ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern =
                command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
        final String tableFilterPattern =
                command.hasTableNameFilterPattern() ? command.getTableNameFilterPattern() : null;

        final ProtocolStringList protocolStringList = command.getTableTypesList();
        final int protocolSize = protocolStringList.size();
        final String[] tableTypes =
                protocolSize == 0 ? null : protocolStringList.toArray(new String[protocolSize]);

        try (final Connection connection = DriverManager.getConnection(getConnection(context));
             final VectorSchemaRoot vectorSchemaRoot = getTablesRoot(
                     connection.getMetaData(),
                     rootAllocator,
                     command.getIncludeSchema(),
                     catalog, schemaFilterPattern, tableFilterPattern, tableTypes)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (SQLException | IOException e) {
            logger.error(format("Failed to getStreamTables: <%s>.", e.getMessage()), e);
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_TABLE_TYPES_SCHEMA);
    }

    private static VectorSchemaRoot getTableTypesRoot(final ResultSet data, final BufferAllocator allocator)
            throws SQLException {
        return getRoot(data, allocator, "table_type", "TABLE_TYPE");
    }

    private static VectorSchemaRoot getCatalogsRoot(final ResultSet data, final BufferAllocator allocator)
            throws SQLException {
        return getRoot(data, allocator, "catalog_name", "TABLE_CATALOG");
    }

    private static VectorSchemaRoot getRoot(final ResultSet data, final BufferAllocator allocator,
                                            final String fieldVectorName, final String columnName)
            throws SQLException {
        final VarCharVector dataVector =
                new VarCharVector(fieldVectorName, FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);
        saveToVectors(ImmutableMap.of(dataVector, columnName), data);
        final int rows = dataVector.getValueCount();
        dataVector.setValueCount(rows);
        return new VectorSchemaRoot(singletonList(dataVector));
    }

    @Override
    public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
        var constr = getConnection(context);
        try (final Connection connection = DriverManager.getConnection(constr);
             final ResultSet tableTypes = connection.getMetaData().getTableTypes();
             final VectorSchemaRoot vectorSchemaRoot = getTableTypesRoot(tableTypes, rootAllocator)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (SQLException e) {
            logger.error(format("Failed to getStreamTableTypes: <%s>.", e.getMessage()), e);
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_PRIMARY_KEYS_SCHEMA);
    }

    @Override
    public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys command, CallContext context, ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schema = command.hasDbSchema() ? command.getDbSchema() : null;
        final String table = command.getTable();

        try (Connection connection = DriverManager.getConnection(DATABASE_URI)) {
            final ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(catalog, schema, table);

            final VarCharVector catalogNameVector = new VarCharVector("catalog_name", rootAllocator);
            final VarCharVector schemaNameVector = new VarCharVector("db_schema_name", rootAllocator);
            final VarCharVector tableNameVector = new VarCharVector("table_name", rootAllocator);
            final VarCharVector columnNameVector = new VarCharVector("column_name", rootAllocator);
            final IntVector keySequenceVector = new IntVector("key_sequence", rootAllocator);
            final VarCharVector keyNameVector = new VarCharVector("key_name", rootAllocator);

            final List<FieldVector> vectors =
                    new ArrayList<>(
                            ImmutableList.of(
                                    catalogNameVector, schemaNameVector, tableNameVector, columnNameVector, keySequenceVector,
                                    keyNameVector));
            vectors.forEach(FieldVector::allocateNew);

            int rows = 0;
            for (; primaryKeys.next(); rows++) {
                saveToVector(primaryKeys.getString("TABLE_CAT"), catalogNameVector, rows);
                saveToVector(primaryKeys.getString("TABLE_SCHEM"), schemaNameVector, rows);
                saveToVector(primaryKeys.getString("TABLE_NAME"), tableNameVector, rows);
                saveToVector(primaryKeys.getString("COLUMN_NAME"), columnNameVector, rows);
                final int key_seq = primaryKeys.getInt("KEY_SEQ");
                saveToVector(primaryKeys.wasNull() ? null : key_seq, keySequenceVector, rows);
                saveToVector(primaryKeys.getString("PK_NAME"), keyNameVector, rows);
            }

            try (final VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors)) {
                vectorSchemaRoot.setRowCount(rows);

                listener.start(vectorSchemaRoot);
                listener.putNext();
            }
        } catch (SQLException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_EXPORTED_KEYS_SCHEMA);
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_IMPORTED_KEYS_SCHEMA);
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_CROSS_REFERENCE_SCHEMA);
    }

    private VectorSchemaRoot createVectors(ResultSet keys) throws SQLException {
        final VarCharVector pkCatalogNameVector = new VarCharVector("pk_catalog_name", rootAllocator);
        final VarCharVector pkSchemaNameVector = new VarCharVector("pk_db_schema_name", rootAllocator);
        final VarCharVector pkTableNameVector = new VarCharVector("pk_table_name", rootAllocator);
        final VarCharVector pkColumnNameVector = new VarCharVector("pk_column_name", rootAllocator);
        final VarCharVector fkCatalogNameVector = new VarCharVector("fk_catalog_name", rootAllocator);
        final VarCharVector fkSchemaNameVector = new VarCharVector("fk_db_schema_name", rootAllocator);
        final VarCharVector fkTableNameVector = new VarCharVector("fk_table_name", rootAllocator);
        final VarCharVector fkColumnNameVector = new VarCharVector("fk_column_name", rootAllocator);
        final IntVector keySequenceVector = new IntVector("key_sequence", rootAllocator);
        final VarCharVector fkKeyNameVector = new VarCharVector("fk_key_name", rootAllocator);
        final VarCharVector pkKeyNameVector = new VarCharVector("pk_key_name", rootAllocator);
        final UInt1Vector updateRuleVector = new UInt1Vector("update_rule", rootAllocator);
        final UInt1Vector deleteRuleVector = new UInt1Vector("delete_rule", rootAllocator);

        Map<FieldVector, String> vectorToColumnName = new HashMap<>();
        vectorToColumnName.put(pkCatalogNameVector, "PKTABLE_CAT");
        vectorToColumnName.put(pkSchemaNameVector, "PKTABLE_SCHEM");
        vectorToColumnName.put(pkTableNameVector, "PKTABLE_NAME");
        vectorToColumnName.put(pkColumnNameVector, "PKCOLUMN_NAME");
        vectorToColumnName.put(fkCatalogNameVector, "FKTABLE_CAT");
        vectorToColumnName.put(fkSchemaNameVector, "FKTABLE_SCHEM");
        vectorToColumnName.put(fkTableNameVector, "FKTABLE_NAME");
        vectorToColumnName.put(fkColumnNameVector, "FKCOLUMN_NAME");
        vectorToColumnName.put(keySequenceVector, "KEY_SEQ");
        vectorToColumnName.put(updateRuleVector, "UPDATE_RULE");
        vectorToColumnName.put(deleteRuleVector, "DELETE_RULE");
        vectorToColumnName.put(fkKeyNameVector, "FK_NAME");
        vectorToColumnName.put(pkKeyNameVector, "PK_NAME");

        final VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(
                pkCatalogNameVector, pkSchemaNameVector, pkTableNameVector, pkColumnNameVector, fkCatalogNameVector,
                fkSchemaNameVector, fkTableNameVector, fkColumnNameVector, keySequenceVector, fkKeyNameVector,
                pkKeyNameVector, updateRuleVector, deleteRuleVector);

        vectorSchemaRoot.allocateNew();
        final int rowCount = saveToVectors(vectorToColumnName, keys, true);

        vectorSchemaRoot.setRowCount(rowCount);

        return vectorSchemaRoot;
    }

    @Override
    public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys command, CallContext context, ServerStreamListener listener) {
        String catalog = command.hasCatalog() ? command.getCatalog() : null;
        String schema = command.hasDbSchema() ? command.getDbSchema() : null;
        String table = command.getTable();

        try (Connection connection = DriverManager.getConnection(DATABASE_URI);
             ResultSet keys = connection.getMetaData().getExportedKeys(catalog, schema, table);
             VectorSchemaRoot vectorSchemaRoot = createVectors(keys)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (SQLException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys command, CallContext context, ServerStreamListener listener) {
        String catalog = command.hasCatalog() ? command.getCatalog() : null;
        String schema = command.hasDbSchema() ? command.getDbSchema() : null;
        String table = command.getTable();

        try (Connection connection = DriverManager.getConnection(DATABASE_URI);
             ResultSet keys = connection.getMetaData().getImportedKeys(catalog, schema, table);
             VectorSchemaRoot vectorSchemaRoot = createVectors(keys)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (final SQLException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public void getStreamCrossReference(FlightSql.CommandGetCrossReference command, CallContext context, ServerStreamListener listener) {
        final String pkCatalog = command.hasPkCatalog() ? command.getPkCatalog() : null;
        final String pkSchema = command.hasPkDbSchema() ? command.getPkDbSchema() : null;
        final String fkCatalog = command.hasFkCatalog() ? command.getFkCatalog() : null;
        final String fkSchema = command.hasFkDbSchema() ? command.getFkDbSchema() : null;
        final String pkTable = command.getPkTable();
        final String fkTable = command.getFkTable();

        try (Connection connection = DriverManager.getConnection(DATABASE_URI);
             ResultSet keys = connection.getMetaData()
                     .getCrossReference(pkCatalog, pkSchema, pkTable, fkCatalog, fkSchema, fkTable);
             VectorSchemaRoot vectorSchemaRoot = createVectors(keys)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (final SQLException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            preparedStatementLoadingCache.cleanUp();
            executorService.shutdown();
        } catch (Throwable t) {
            logger.error(format("Failed to close resources: <%s>", t.getMessage()), t);
        }

        AutoCloseables.close(rootAllocator);
    }
}
