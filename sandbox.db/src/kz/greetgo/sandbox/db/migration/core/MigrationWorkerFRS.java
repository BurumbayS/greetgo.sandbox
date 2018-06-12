package kz.greetgo.sandbox.db.migration.core;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class MigrationWorkerFRS extends MigrationWorker{

  private InputStream inputStream;
  private OutputStream errorOutStream;

  private int batchSize = 0;

  private String tmpAccountTable, tmpTransactionTable;

  public String r(String sql) {
    sql = sql.replaceAll("TMP_ACCOUNT", tmpAccountTable);
    sql = sql.replaceAll("TMP_TRANSACTION", tmpTransactionTable);
    return sql;
  }

  public MigrationWorkerFRS(Connection connection, InputStream inputStream, OutputStream errorOutStream, int batchSize) {
    this.connection = connection;
    this.inputStream = inputStream;
    this.errorOutStream = errorOutStream;
    this.batchSize = batchSize;

    tmpAccountTable = "frs_migration_account_";
    tmpTransactionTable = "frs_migration_transaction_";
  }

  public void migrate() throws Exception {

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

        FromJSONParser fromJSONParser = new FromJSONParser();
        fromJSONParser.execute(connection, accountPS, transPS, batchSize);

        recordsCount = fromJSONParser.parseRecordData(inputStream);

        if (fromJSONParser.getAccBatchSize() > 0 || fromJSONParser.getTransBatchSize() > 0) {
          accountPS.executeBatch();
          transPS.executeBatch();
          connection.commit();
        }
      }

    } finally {
      connection.setAutoCommit(true);
    }
  }

  @SuppressWarnings("SqlResolve")
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

    //language=PostgreSQL
    exec("UPDATE TMP_ACCOUNT tmp SET client_id = c.id\n" +
      "FROM tmp_clients c\n" +
      "WHERE tmp.client_cia_id = c.cia_id AND tmp.status = 0");

    //language=PostgreSQL
    exec("UPDATE TMP_ACCOUNT SET status = 1, error = 'client_id is not defined'\n" +
      "WHERE client_id IS NULL AND status = 0");
  }

  @SuppressWarnings("SqlResolve")
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
    exec("UPDATE TMP_TRANSACTION SET status = 1, error = 'account id is not defined'\n" +
      "WHERE account_id IS NULL AND status = 0");

    //language=PostgreSQL
    exec("INSERT INTO tmp_transactions (money, finished_at, account_id, transaction_type_id)\n" +
      "SELECT money, finished_at, account_id, transaction_type_id \n" +
      "FROM TMP_TRANSACTION tmp\n" +
      "WHERE tmp.status = 0");

    uploadErrorsToFile();
  }

  private void uploadErrorsToFile() throws Exception {

    try (OutputStreamWriter writer = new OutputStreamWriter(errorOutStream)) {
      uploadErrors("TMP_TRANSACTION", writer);
      uploadErrors("TMP_ACCOUNT", writer);
    }
  }
}
