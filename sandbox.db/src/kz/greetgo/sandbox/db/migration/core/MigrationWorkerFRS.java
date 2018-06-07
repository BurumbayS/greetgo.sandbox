package kz.greetgo.sandbox.db.migration.core;

import kz.greetgo.sandbox.db.util.App;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
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

public class MigrationWorkerFRS {

  private Connection connection;
  private InputStream inputSream;
  private OutputStream errorOutStream;

  private int batchSize = 0;

  private String tmpAccountTable, tmpTransactionTable;

  private Map<String , String> sqlRequests = new TreeMap<>();

  private void info(String message) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    System.out.println(sdf.format(new Date()) + " [" + getClass().getSimpleName() + "] " + message);
  }

  private String r(String sql) {
    sql = sql.replaceAll("TMP_ACCOUNT", tmpAccountTable);
    sql = sql.replaceAll("TMP_TRANSACTION", tmpTransactionTable);
    return sql;
  }

  private void exec(String sql) throws SQLException {
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

  public void migrate(Connection connection, InputStream inputSream, OutputStream errorOutStream, int batchSize) throws Exception {

    //Вынеси инициализацию в конструктор
    this.connection = connection;
    this.inputSream = inputSream;
    this.errorOutStream = errorOutStream;
    this.batchSize = batchSize;

    tmpAccountTable = "frs_migration_account_";
    tmpTransactionTable = "frs_migration_transaction_";

    createTmpTables();

    download();

    verification();

    migrateFromTmp();
  }

  public void createTmpTables() throws Exception {

    //language=PostgreSQL
    exec("CREATE TABLE TMP_ACCOUNT (\n" +
      "  status INT NOT NULL DEFAULT 0,\n" +
      "  error VARCHAR(300),\n" +
      "  line BIGINT,\n" +
      "  \n" +
      "  number BIGSERIAL PRIMARY KEY,\n" +
      "  account_number VARCHAR(100),\n" +
      "  registered_at TIMESTAMP,\n" +
      "  client_cia_id VARCHAR(100),\n" +
      "  client_id VARCHAR(100)\n" +
      ")");

    //language=PostgreSQL
    exec("CREATE TABLE TMP_TRANSACTION (\n" +
      "  status INT NOT NULL DEFAULT 0,\n" +
      "  error VARCHAR(300),\n" +
      "  line BIGINT,\n" +
      "  \n" +
      "  number BIGSERIAL PRIMARY KEY,\n" +
      "  money FLOAT,\n" +
      "  account_number VARCHAR(100),\n" +
      "  account_id BIGINT,\n" +
      "  finished_at TIMESTAMP,\n" +
      "  transaction_type VARCHAR(300),\n" +
      "  transaction_type_id BIGINT\n" +
      ")");
  }

  public void download() throws Exception {

    Insert account_insert = new Insert("TMP_ACCOUNT");
    account_insert.field(1, "account_number", "?");
    account_insert.field(2, "registered_at", "?");
    account_insert.field(3, "client_cia_id", "?");
    account_insert.field(4, "line", "?");

    Insert transaction_insert = new Insert("TMP_TRANSACTION");
    transaction_insert.field(1, "money", "?");
    transaction_insert.field(2, "account_number", "?");
    transaction_insert.field(3, "finished_at", "?");
    transaction_insert.field(4, "transaction_type", "?");
    transaction_insert.field(5, "line", "?");

    connection.setAutoCommit(false);
    try (PreparedStatement accountPS = connection.prepareStatement(r(account_insert.toString()))) {

      try (PreparedStatement transPS = connection.prepareStatement(r(transaction_insert.toString()))) {

        int recordsCount = 0;

        FromJSONParser fromJSONParser = new FromJSONParser();
        fromJSONParser.execute(connection, accountPS, transPS, batchSize);

        recordsCount = fromJSONParser.parseRecordData(inputSream);

        if (fromJSONParser.getAccBatchSize() > 0 || fromJSONParser.getTransBatchSize() > 0) {
          accountPS.executeBatch();
          transPS.executeBatch();
          connection.commit();
        }

//        return recordsCount;
      }

    } finally {
      connection.setAutoCommit(true);
    }
  }

  public void verification() throws Exception {
    //language=PostgreSQL
    exec("UPDATE TMP_TRANSACTION SET error = 'transaction type is not defined', status = 1\n" +
      "WHERE error IS NULL AND transaction_type IS NULL");
    //language=PostgreSQL
    exec("UPDATE TMP_TRANSACTION SET error = 'account number is not defined', status = 1\n" +
      "WHERE error IS NULL AND account_number IS NULL");
    //language=PostgreSQL
    exec("UPDATE TMP_ACCOUNT SET error = 'cia_id is not defined', status = 1\n" +
      "WHERE error IS NULL AND client_cia_id IS NULL");
    //language=PostgreSQL
    exec("UPDATE TMP_ACCOUNT SET error = 'account number is not defined', status = 1\n" +
      "WHERE error IS NULL AND account_number IS NULL");

    uploadErrors();

    //language=PostgreSQL
    exec("UPDATE TMP_ACCOUNT tmp SET client_id = c.id\n" +
      "FROM tmp_clients c\n" +
      "WHERE tmp.client_cia_id = c.cia_id AND tmp.status = 0");

    //language=PostgreSQL
    exec("UPDATE TMP_ACCOUNT SET status = 1\n" +
      "WHERE client_id IS NULL AND status = 0");
  }

  public void migrateFromTmp() throws Exception {

    //language=PostgreSQL
    exec("INSERT INTO tmp_accounts (number, registered_at, client_id)\n" +
      "SELECT account_number, registered_at, client_id \n" +
      "FROM TMP_ACCOUNT tmp\n" +
      "WHERE tmp.client_id IS NOT NULL AND tmp.status = 0");

    //language=PostgreSQL
    exec("INSERT INTO tmp_transaction_types (name)\n" +
      "SELECT transaction_type \n" +
      "FROM TMP_TRANSACTION tmp\n" +
      "WHERE tmp.transaction_type NOT IN (SELECT name FROM tmp_transaction_types) AND tmp.status = 0" +
      "GROUP BY tmp.transaction_type");

    //language=PostgreSQL
    exec("UPDATE TMP_TRANSACTION tmp SET transaction_type_id = t.id\n" +
      "FROM tmp_transaction_types t\n" +
      "WHERE tmp.transaction_type = t.name AND tmp.status = 0");

    //language=PostgreSQL
    exec("UPDATE TMP_TRANSACTION tmp SET account_id = acc.id\n" +
      "FROM tmp_accounts acc\n" +
      "WHERE tmp.account_number = acc.number AND tmp.status = 0");

    //language=PostgreSQL
    exec("UPDATE TMP_TRANSACTION SET status = 1\n" +
      "WHERE account_id IS NULL AND status = 0");

    //language=PostgreSQL
    exec("INSERT INTO tmp_transactions (money, finished_at, account_id, transaction_type_id)\n" +
      "SELECT money, finished_at, account_id, transaction_type_id \n" +
      "FROM TMP_TRANSACTION tmp\n" +
      "WHERE tmp.status = 0");
  }

  private void uploadErrors() throws Exception {
    File file = new File(App.appDir() + "/frsErrors.txt");
    OutputStream out = new FileOutputStream(file);
    OutputStreamWriter writer = new OutputStreamWriter(out);

    String sql = "select line, error from TMP_TRANSACTION where status = 1";
    uploadErrorsFromDB(sql, writer);

    sql = "select line, error from TMP_ACCOUNT where status = 1";
    uploadErrorsFromDB(sql, writer);

    writer.close();
  }
  private void uploadErrorsFromDB(String sql, OutputStreamWriter writer) throws Exception{
    try (PreparedStatement ps = connection.prepareStatement(r(sql))) {

      try (ResultSet rs = ps.executeQuery()) {

        int cnt = 0;
        while (rs.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          stringBuilder.append(cnt);
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
}
