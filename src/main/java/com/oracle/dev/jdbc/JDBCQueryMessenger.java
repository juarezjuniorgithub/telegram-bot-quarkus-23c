package com.oracle.dev.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import oracle.jdbc.pool.OracleDataSource;

@ApplicationScoped
public class JDBCQueryMessenger {

	private static final Logger logger = LoggerFactory.getLogger(JDBCQueryMessenger.class);

	@ConfigProperty(name = "jdbc.url")
	private String jdbcUrl;

	@ConfigProperty(name = "jdbc.username")
	private String jdbcUserName;

	@ConfigProperty(name = "jdbc.password")
	private String jdbcPassword;

	@ConfigProperty(name = "jdbc.query")
	private String jdbcQuery;

	@Inject
	private OracleTelegramBot bot;

	private OracleDataSource ods;
	private Connection conn;

	@Scheduled(every = "10s")
	public void sendQueryResults() {
		if (conn == null) {
			initConnection();
		}
		bot.sendMessage(query());
	}

	private String query() {
		StringBuilder queryResults = new StringBuilder();
		try {
			PreparedStatement stmt = conn.prepareStatement(jdbcQuery);
			stmt.setInt(1, randomize());
			ResultSet rslt = stmt.executeQuery();
			while (rslt.next()) {
				queryResults.append(rslt.getString("TIP"));
				logger.info(queryResults.toString());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return queryResults.toString();
	}

	private void initConnection() {
		try {
			ods = new OracleDataSource();
			ods.setURL(jdbcUrl);
			ods.setUser(jdbcUserName);
			ods.setPassword(jdbcPassword);
			conn = ods.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();

		}
	}

	private int randomize() {
		return ThreadLocalRandom.current().nextInt(1, 13 + 1);
	}

}
