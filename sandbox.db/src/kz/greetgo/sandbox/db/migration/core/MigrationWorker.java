package kz.greetgo.sandbox.db.migration.core;

import kz.greetgo.sandbox.db.util.App;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import static kz.greetgo.sandbox.db.migration.util.TimeUtils.showTime;

public abstract class MigrationWorker {
  Connection connection;

  int recordsCount = 0;

  Map<String , String> sqlRequests = new TreeMap<>();

  private void info(String message) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    System.out.println(sdf.format(new Date()) + " [" + getClass().getSimpleName() + "] " + message);
  }

  abstract String r(String sql);

  public void exec(String sql) throws SQLException {
    String executingSql = r(sql);

    long startedAt = System.nanoTime();

    try (Statement statement = connection.createStatement()) {
      int updates = statement.executeUpdate(executingSql);
      info("Updated " + updates
        + " records for " + showTime(System.nanoTime(), startedAt)
        + ", EXECUTED SQL : " + executingSql);
    } catch (SQLException e) {
      info("ERROR EXECUTE SQL for " + showTime(System.nanoTime(), startedAt)
        + ", message: " + e.getMessage() + ", SQL : " + executingSql);
      throw e;
    }

    sqlRequests.put(showTime(System.nanoTime(), startedAt), sql);
  }

  public void uploadErrors(String tableName, OutputStreamWriter writer) throws Exception {
    String sql = "select line, error from " + tableName + " where status = 1";

    try (PreparedStatement ps = connection.prepareStatement(r(sql))) {

      try (ResultSet rs = ps.executeQuery()) {

        int cnt = 0;
        while (rs.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          stringBuilder.append(cnt++);
          stringBuilder.append(". Line: ");
          stringBuilder.append(rs.getLong("line"));
          stringBuilder.append("    Error: ");
          stringBuilder.append(rs.getString("error"));
          stringBuilder.append("\n");

          writer.write(stringBuilder.toString());
        }
      }
    }
  }

  public Map<String,String> getSqlRequests() {
    return sqlRequests;
  }

  public int getRecordsCount() {
    return recordsCount;
  }

}
