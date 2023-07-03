package com.netapp.spark;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.sql.CancelResult;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;

public class SparkSQLProducer implements FlightSqlProducer {
    static Logger logger = LoggerFactory.getLogger(SparkSQLProducer.class);
    SparkSession sparkSession;

    public SparkSQLProducer(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static void main(String[] args) {
        try (var connection = DriverManager.getConnection("jdbc:arrow-flight-sql://localhost:33333/?useEncryption=false");
             var statement = connection.createStatement()) {
            var resultSet = statement.executeQuery("SELECT * FROM global_temp.spark_connect_info");
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

    @Override
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        var query = request.getQuery();
        var df = sparkSession.sql(query);
        df.toLocalIterator().forEachRemaining(row -> {
            //listener.onNext(Flight.Result.newBuilder().);
        });
        listener.onCompleted();
    }

    @Override
    public void createPreparedSubstraitPlan(FlightSql.ActionCreatePreparedSubstraitPlanRequest request, CallContext context, StreamListener<FlightSql.ActionCreatePreparedStatementResult> listener) {
        FlightSqlProducer.super.createPreparedSubstraitPlan(request, context, listener);
    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {

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
        return null;
    }

    @Override
    public FlightInfo getFlightInfoSubstraitPlan(FlightSql.CommandStatementSubstraitPlan command, CallContext context, FlightDescriptor descriptor) {
        return FlightSqlProducer.super.getFlightInfoSubstraitPlan(command, context, descriptor);
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        return null;
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
    public void getStreamStatement(FlightSql.TicketStatementQuery ticket, CallContext context, ServerStreamListener listener) {

    }

    @Override
    public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context, ServerStreamListener listener) {

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

    @Override
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request, CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command, CallContext context, ServerStreamListener listener) {

    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context, ServerStreamListener listener) {

    }

    @Override
    public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs request, CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {

    }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request, CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas command, CallContext context, ServerStreamListener listener) {

    }

    @Override
    public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables request, CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamTables(FlightSql.CommandGetTables command, CallContext context, ServerStreamListener listener) {

    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request, CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {

    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys request, CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys command, CallContext context, ServerStreamListener listener) {

    }

    @Override
    public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys request, CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys request, CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference request, CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys command, CallContext context, ServerStreamListener listener) {

    }

    @Override
    public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys command, CallContext context, ServerStreamListener listener) {

    }

    @Override
    public void getStreamCrossReference(FlightSql.CommandGetCrossReference command, CallContext context, ServerStreamListener listener) {

    }

    @Override
    public void close() throws Exception {

    }
}
