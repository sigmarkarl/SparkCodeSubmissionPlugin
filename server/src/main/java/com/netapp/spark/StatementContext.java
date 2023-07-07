package com.netapp.spark;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Objects;

import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.util.AutoCloseables;

/**
 * Context for {@link T} to be persisted in memory in between {@link FlightSqlProducer} calls.
 *
 * @param <T> the {@link Statement} to be persisted.
 */
public final class StatementContext<T extends Statement> implements AutoCloseable {

    private final T statement;
    private final String query;

    public StatementContext(final T statement, final String query) {
        this.statement = Objects.requireNonNull(statement, "statement cannot be null.");
        this.query = query;
    }

    public StatementContext(final String query) {
        this.query = query;
        this.statement = null;
    }

    /**
     * Gets the statement wrapped by this {@link StatementContext}.
     *
     * @return the inner statement.
     */
    public T getStatement() {
        return statement;
    }

    /**
     * Gets the optional SQL query wrapped by this {@link StatementContext}.
     *
     * @return the SQL query if present; empty otherwise.
     */
    public String getQuery() {
        return query;
    }

    @Override
    public void close() throws Exception {
        if (statement == null) {
            return;
        }
        Connection connection = statement.getConnection();
        AutoCloseables.close(statement, connection);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StatementContext)) {
            return false;
        }
        final StatementContext<?> that = (StatementContext<?>) other;
        return statement.equals(that.statement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statement);
    }
}
