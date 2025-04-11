package liquibase.ext.cassandra.database;

import com.ing.data.cassandra.jdbc.CassandraDriver;
import liquibase.Scope;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;

import java.net.URI;
import java.sql.Driver;
import java.util.Properties;

public class CassandraDatabaseConnection extends JdbcConnection {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE + 200;
    }

    @Override
    public boolean supports(String url) {
        return url.toLowerCase().startsWith("jdbc:cassandra");
    }

    @Override
    public void open(String url, Driver driverObject, Properties driverProperties) throws DatabaseException {
        String jdbcUrl = url;

        // When using Cassandra with com.ing.data.cassandra.jdbc.CassandraDriver, it is required to specify the
        // compliance mode "Liquibase" in the JDBC URL. So, do it by default when it's necessary.
        if (driverObject instanceof CassandraDriver) {
            try {
                boolean complianceModePresent = false;
                final String liquibaseComplianceModeParameter = "compliancemode=Liquibase";
                // Replace the actual protocol (jdbc:cassandra:// or any other) by a valid protocol for URI
                // parsing (jdbc://)
                final int protocolEndIdx = jdbcUrl.indexOf("://");
                final String parseableUrl = "jdbc" + jdbcUrl.substring(protocolEndIdx);
                final URI jdbcUri = URI.create(parseableUrl);
                final String queryPart = jdbcUri.getQuery();
                if (queryPart != null) {
                    String[] queryParams = queryPart.split("&");
                    for (String queryParam : queryParams) {
                        if (liquibaseComplianceModeParameter.equals(queryParam)) {
                            complianceModePresent = true;
                            break;
                        }
                    }
                    if (!complianceModePresent) {
                        jdbcUrl += "&" + liquibaseComplianceModeParameter;
                    }
                } else {
                    jdbcUrl += "?" + liquibaseComplianceModeParameter;
                }
                Scope.getCurrentScope().getLog(CassandraDatabaseConnection.class)
                        .info("Connecting to Cassandra using: " + jdbcUrl);
            } catch (IllegalArgumentException e) {
                Scope.getCurrentScope().getLog(CassandraDatabaseConnection.class)
                        .warning("Unable to check compliance mode in JDBC URL, connecting with configured URL. "
                                + "The compliance mode might be incorrect.");
            }
        }

        openConnection(jdbcUrl, driverObject, driverProperties);
    }

    void openConnection(String url, Driver driverObject, Properties driverProperties) throws DatabaseException {
        super.open(url, driverObject, driverProperties);
    }

}
