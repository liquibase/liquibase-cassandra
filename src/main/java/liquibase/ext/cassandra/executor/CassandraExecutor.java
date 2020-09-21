package liquibase.ext.cassandra.executor;

import java.sql.Statement;
import java.util.List;

import liquibase.Scope;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.executor.jvm.JdbcExecutor;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.statement.SqlStatement;

public class CassandraExecutor extends JdbcExecutor {

	@Override
	public int getPriority() {
		return PRIORITY_SPECIALIZED;
	}

	@Override
	public void execute(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) throws DatabaseException {

		DatabaseConnection con = this.database.getConnection();

		try (Statement stmt = ((JdbcConnection) con).getUnderlyingConnection().createStatement()) {

			for (String statement : applyVisitors(sql, sqlVisitors)) {
				if (!stmt.execute(statement)) {
					Scope.getCurrentScope().getLog(getClass())
							.fine(Integer.toString(stmt.getUpdateCount()) + " row(s) affected");
				}
			}

		} catch (Throwable e) {

			throw new DatabaseException(e.getMessage());
		}

	}

}
