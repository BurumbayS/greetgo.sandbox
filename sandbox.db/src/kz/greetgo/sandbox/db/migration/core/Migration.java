package kz.greetgo.sandbox.db.migration.core;

import org.xml.sax.SAXException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static kz.greetgo.sandbox.db.migration.util.TimeUtils.showTime;


public class Migration implements Closeable {

  private Connection connection = null;
  private List<String> frsFiles;
  private List<String> ciaFiles;
  private Map<String, String> sqlRequests = new TreeMap<>();

  public Migration(Connection connection, List<String> frsFiles, List<String> ciaFiles) {
    this.connection = connection;
    this.frsFiles = frsFiles;
    this.ciaFiles = ciaFiles;

    tmpClientTable = "cia_migration_client_";
    tmpPhoneTable = "cia_migration_phone_";
    tmpAccountTable = "frs_migration_account_";
    tmpTransactionTable = "frs_migration_transaction_";
    info("TMP_CLIENT = " + tmpClientTable);
    info("TMP_PHONE = " + tmpPhoneTable);
    info("TMP_ACCOUNT = " + tmpAccountTable);
    info("TMP_TRANSACTION = " + tmpTransactionTable);
  }

  @Override
  public void close() {
    closeOperConnection();
  }

  private void closeOperConnection() {
    if (this.connection != null) {
      try {
        this.connection.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      this.connection = null;
    }
  }

  private void info(String message) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    System.out.println(sdf.format(new Date()) + " [" + getClass().getSimpleName() + "] " + message);
  }

  private String r(String sql) {
    sql = sql.replaceAll("TMP_CLIENT", tmpClientTable);
    sql = sql.replaceAll("TMP_PHONE", tmpPhoneTable);
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

    sqlRequests.put(showTime(System.nanoTime(), startedAt), executingSql);
  }

  public int portionSize = 10_000_000;
  public int downloadMaxBatchSize = 30_000;
  public int uploadMaxBatchSize = 30_000;
  public int showStatusPingMillis = 5000;

  private String tmpClientTable, tmpPhoneTable;
  private String tmpAccountTable, tmpTransactionTable;

  public int execute() throws Exception {
    long startedAt = System.nanoTime();

    createTmpTables();

    int portionSize = 0;

    for (int i = 0; i < Integer.max(frsFiles.size(), ciaFiles.size()); i++) {
      if (i < ciaFiles.size()) {
        portionSize = downloadFromCIA(ciaFiles.get(i));
        {
          long now = System.nanoTime();
          info("Downloaded of portion " + portionSize + " from CIA finished for " + showTime(now, startedAt));
        }
      }

      if (i < frsFiles.size()) {
        portionSize += downloadFromFRS(frsFiles.get(i));
        {
          long now = System.nanoTime();
          info("Downloaded of portion " + portionSize + " from FRS finished for " + showTime(now, startedAt));
        }
      }
    }

    migrateFromTmp();

    {
      long now = System.nanoTime();
      info("Migration of portion " + portionSize + " finished for " + showTime(now, startedAt));
    }

    deleteTables();

//    for(String key : sqlRequests.keySet()) {
//      System.out.print(key + " ");
//      System.out.println(sqlRequests.get(key));
//    }
    return portionSize;
  }

  public void createTmpTables() throws Exception {

    //language=PostgreSQL
    exec("CREATE TABLE TMP_CLIENT (\n" +
      "  client_id VARCHAR(20),\n" +
      "  status INT NOT NULL DEFAULT 0,\n" +
      "  error VARCHAR(300),\n" +
      "  \n" +
      "  number BIGSERIAL PRIMARY KEY,\n" +
      "  id VARCHAR(100) NOT NULL,\n" +
      "  cia_id VARCHAR(100) NOT NULL,\n" +
      "  surname VARCHAR(300),\n" +
      "  name VARCHAR(300),\n" +
      "  patronymic VARCHAR(300),\n" +
      "  charm VARCHAR(300),\n" +
      "  gender VARCHAR(300),\n" +
      "  birth_date DATE,\n" +
      "  rStreet VARCHAR(100),\n" +
      "  rHouse VARCHAR(100),\n" +
      "  rFlat VARCHAR(100),\n" +
      "  fStreet VARCHAR(100),\n" +
      "  fHouse VARCHAR(100),\n" +
      "  fFlat VARCHAR(100)\n" +
      ")");

    //language=PostgreSQL
    exec("CREATE TABLE TMP_PHONE (\n" +
      "  status INT NOT NULL DEFAULT 0,\n" +
      "  error VARCHAR(300),\n" +
      "  \n" +
      "  num BIGSERIAL PRIMARY KEY,\n" +
      "  cia_id VARCHAR(100) NOT NULL,\n" +
      "  tmp_client_id VARCHAR(100) NOT NULL,\n" +
      "  number VARCHAR(100),\n" +
      "  phoneType VARCHAR(20),\n" +
      "  client_id VARCHAR(100)\n" +
      ")");

    //language=PostgreSQL
    exec("CREATE TABLE TMP_ACCOUNT (\n" +
      "  status INT NOT NULL DEFAULT 0,\n" +
      "  error VARCHAR(300),\n" +
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

  public int downloadFromCIA(String filePath) throws SQLException, IOException, SAXException {

//    final AtomicBoolean working = new AtomicBoolean(true);
//    final AtomicBoolean showStatus = new AtomicBoolean(false);
//
//    final Thread see = new Thread(() -> {
//
//      while (working.get()) {
//
//        try {
//          Thread.sleep(showStatusPingMillis);
//        } catch (InterruptedException e) {
//          break;
//        }
//
//        showStatus.set(true);
//
//      }
//
//    });
//    see.start();

    Insert client_insert = new Insert("TMP_CLIENT");
    client_insert.field(1, "cia_id", "?");
    client_insert.field(2, "surname", "?");
    client_insert.field(3, "name", "?");
    client_insert.field(4, "patronymic", "?");
    client_insert.field(5, "gender", "?");
    client_insert.field(6, "charm", "?");
    client_insert.field(7, "birth_date", "?");
    client_insert.field(8, "id", "?");
    client_insert.field(9, "rStreet", "?");
    client_insert.field(10, "rHouse", "?");
    client_insert.field(11, "rFlat", "?");
    client_insert.field(12, "fStreet", "?");
    client_insert.field(13, "fHouse", "?");
    client_insert.field(14, "fFlat", "?");

    Insert phone_insert = new Insert("TMP_PHONE");
    phone_insert.field(1, "cia_id", "?");
    phone_insert.field(2, "number", "?");
    phone_insert.field(3, "phoneType", "?");
    phone_insert.field(4, "tmp_client_id", "?");

    connection.setAutoCommit(false);
    try (PreparedStatement clientPS = connection.prepareStatement(r(client_insert.toString()))) {

      try (PreparedStatement phonePS = connection.prepareStatement(r(phone_insert.toString()))) {

        int recordsCount = 0;

        FromXMLParser fromXMLParser = new FromXMLParser();
        fromXMLParser.execute(connection, clientPS, phonePS, downloadMaxBatchSize);

        try {

          File inputFile = new File(filePath);
          recordsCount = fromXMLParser.parseRecordData(String.valueOf(inputFile));

        } catch (Exception e) {
          e.printStackTrace();
        }

        if (fromXMLParser.getClientBatchSize() > 0 || fromXMLParser.getPhoneBatchSize() > 0) {
          phonePS.executeBatch();
          clientPS.executeBatch();
          connection.commit();
        }


        return recordsCount;
      }

    } finally {
      connection.setAutoCommit(true);
//      working.set(false);
//      see.interrupt();
    }
  }

  public int downloadFromFRS(String filePath) throws SQLException, IOException, SAXException {

//    final AtomicBoolean working = new AtomicBoolean(true);
//    final AtomicBoolean showStatus = new AtomicBoolean(false);
//
//    final Thread see = new Thread(() -> {
//
//      while (working.get()) {
//
//        try {
//          Thread.sleep(showStatusPingMillis);
//        } catch (InterruptedException e) {
//          break;
//        }
//
//        showStatus.set(true);
//
//      }
//
//    });
//    see.start();

    Insert account_insert = new Insert("TMP_ACCOUNT");
    account_insert.field(1, "account_number", "?");
    account_insert.field(2, "registered_at", "?");
    account_insert.field(3, "client_cia_id", "?");

    Insert transaction_insert = new Insert("TMP_TRANSACTION");
    transaction_insert.field(1, "money", "?");
    transaction_insert.field(2, "account_number", "?");
    transaction_insert.field(3, "finished_at", "?");
    transaction_insert.field(4, "transaction_type", "?");

    connection.setAutoCommit(false);
    try (PreparedStatement accountPS = connection.prepareStatement(r(account_insert.toString()))) {

      try (PreparedStatement transPS = connection.prepareStatement(r(transaction_insert.toString()))) {

        int recordsCount = 0;

        FromJSONParser fromJSONParser = new FromJSONParser();
        fromJSONParser.execute(connection, accountPS, transPS, uploadMaxBatchSize);


        try {

          File inputFile = new File(filePath);
          recordsCount = fromJSONParser.parseRecordData(inputFile);

        } catch (Exception e) {
          e.printStackTrace();
        }

        if (fromJSONParser.getAccBatchSize() > 0 || fromJSONParser.getTransBatchSize() > 0) {
          accountPS.executeBatch();
          transPS.executeBatch();
          connection.commit();
        }


        return recordsCount;
      }

    } finally {
      connection.setAutoCommit(true);
//      working.set(false);
//      see.interrupt();
    }
  }

  public void migrateFromTmp() throws Exception {

    //language=PostgreSQL
    exec("UPDATE TMP_CLIENT SET error = 'surname is not defined', status = 1\n" +
      "WHERE error IS NULL AND surname IS NULL");
    //language=PostgreSQL
    exec("UPDATE TMP_CLIENT SET error = 'name is not defined', status = 1\n" +
      "WHERE error IS NULL AND name IS NULL");
    //language=PostgreSQL
    exec("UPDATE TMP_CLIENT SET error = 'birth_date is not defined', status = 1\n" +
      "WHERE error IS NULL AND birth_date IS NULL");
    //language=PostgreSQL
    exec("UPDATE TMP_CLIENT SET error = 'gender is not defined', status = 1\n" +
      "WHERE error IS NULL AND gender IS NULL");
    //language=PostgreSQL
    exec("UPDATE TMP_CLIENT SET error = 'charm is not defined', status = 1\n" +
      "WHERE error IS NULL AND charm IS NULL");
    //language=PostgreSQL
    exec("UPDATE TMP_PHONE SET error = 'number is not defined', status = 1\n" +
      "WHERE error IS NULL AND number IS NULL");
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
    exec("UPDATE TMP_PHONE ph SET status = 1" +
      " FROM TMP_CLIENT cl WHERE cl.id = ph.tmp_client_id AND cl.status = 1");

    //language=PostgreSQL
    exec("WITH num_ord AS (\n" +
      "  SELECT number, cia_id, row_number() OVER(PARTITION BY cia_id ORDER BY number DESC) AS ord \n" +
      "  FROM TMP_CLIENT\n" +
      ")\n" +
      "\n" +
      "UPDATE TMP_CLIENT SET status = 2\n" +
      "WHERE status = 0 AND number IN (SELECT number FROM num_ord WHERE ord > 1)");

    //language=PostgreSQL
    exec("UPDATE TMP_PHONE ph SET status = 2" +
      " FROM TMP_CLIENT cl WHERE cl.id = ph.tmp_client_id AND cl.status = 2");

    //language=PostgreSQL
    exec("UPDATE TMP_CLIENT t SET client_id = c.id\n" +
      "  FROM tmp_clients c\n" +
      "  WHERE c.cia_id = t.cia_id\n");

    //language=PostgreSQL
    exec("UPDATE TMP_PHONE t SET client_id = ph.client_id\n" +
      "  FROM tmp_phones ph\n" +
      "  WHERE ph.cia_id = t.cia_id AND ph.phonetype = t.phoneType\n");

    //language=PostgreSQL
    exec("UPDATE TMP_CLIENT SET status = 3 WHERE client_id IS NOT NULL AND status = 0");

    //language=PostgreSQL
    exec("UPDATE TMP_CLIENT SET client_id = nextval('s_client') WHERE status = 0");

    //language=PostgreSQL
    exec("UPDATE TMP_PHONE SET client_id = cl.client_id " +
      "FROM TMP_CLIENT cl WHERE cl.client_id IS NOT NULL AND tmp_client_id = cl.id" +
      " AND (cl.status = 0 OR cl.status = 3)");

    //language=PostgreSQL
    exec("INSERT INTO tmp_clients (id, cia_id, surname, name, patronymic, birth_date, charm, gender)\n" +
      "SELECT client_id, cia_id, surname, name, patronymic, birth_date, charm, gender\n" +
      "FROM TMP_CLIENT WHERE status = 0");

    //language=PostgreSQL
    exec("INSERT INTO tmp_phones (number, phoneType, client_id, cia_id)\n" +
      "SELECT number, phoneType, client_id, cia_id\n" +
      "FROM TMP_PHONE WHERE status = 0 " +
      "ON CONFLICT DO NOTHING");

    //language=PostgreSQL
    exec("UPDATE tmp_clients c SET surname = s.surname\n" +
      "                 , name = s.name\n" +
      "                 , patronymic = s.patronymic\n" +
      "                 , birth_date = s.birth_date\n" +
      "                 , charm = s.charm\n" +
      "                 , gender = s.gender\n" +
      "FROM TMP_CLIENT s\n" +
      "WHERE c.id = s.client_id\n" +
      "AND s.status = 3");

    //language=PostgreSQL
    exec("INSERT INTO tmp_adresses (street, house, flat, client_id)\n" +
      "SELECT  fStreet, fHouse, fFlat, client_id\n" +
      "FROM TMP_CLIENT cl \n" +
      "WHERE cl.status = 0 AND cl.fStreet IS NOT NULL AND cl.fHouse IS NOT NULL AND cl.fFlat IS NOT NULL");
    //language=PostgreSQL
    exec("UPDATE tmp_adresses SET adresstype = 'FACT'\n" +
      "WHERE adresstype = 'NONE'");
    //language=PostgreSQL
    exec("UPDATE tmp_adresses a SET street = cl.fStreet,\n" +
      "                            house = cl.fHouse,\n" +
      "                            flat = cl.fFlat,\n" +
      "                            client_id = cl.client_id,\n" +
      "                            adresstype = 'FACT'\n" +
      "FROM TMP_CLIENT cl \n" +
      "WHERE cl.status = 3 AND cl.fStreet IS NOT NULL AND cl.fHouse IS NOT NULL AND cl.fFlat IS NOT NULL");

    //language=PostgreSQL
    exec("INSERT INTO tmp_adresses (street, house, flat, client_id)\n" +
      "SELECT  rStreet, rHouse, rFlat, client_id\n" +
      "FROM TMP_CLIENT cl \n" +
      "WHERE cl.status = 0 AND cl.rStreet IS NOT NULL AND cl.rHouse IS NOT NULL AND cl.rFlat IS NOT NULL");
    //language=PostgreSQL
    exec("UPDATE tmp_adresses SET adresstype = 'REG'\n" +
      "WHERE adresstype = 'NONE'");
    //language=PostgreSQL
    exec("UPDATE tmp_adresses a SET street = cl.rStreet,\n" +
      "                            house = cl.rHouse,\n" +
      "                            flat = cl.rFlat,\n" +
      "                            client_id = cl.client_id,\n" +
      "                            adresstype = 'REG'\n" +
      "FROM TMP_CLIENT cl \n" +
      "WHERE cl.status = 3 AND cl.rStreet IS NOT NULL AND cl.rHouse IS NOT NULL AND cl.rFlat IS NOT NULL");

    //language=PostgreSQL
    exec("UPDATE TMP_ACCOUNT tmp SET client_id = c.id\n" +
      "FROM tmp_clients c\n" +
      "WHERE tmp.client_cia_id = c.cia_id AND tmp.status = 0");

    //language=PostgreSQL
    exec("UPDATE TMP_ACCOUNT SET status = 1\n" +
      "WHERE client_id IS NULL AND status = 0");

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

    //language=PostgreSQL
    exec("UPDATE tmp_clients SET actual = 1 WHERE id IN (\n" +
      "  SELECT client_id FROM TMP_CLIENT WHERE status = 0\n" +
      ")");
  }

  private void deleteTables() throws Exception {
    String sql = "CREATE OR REPLACE FUNCTION removeTables()\n" +
      "  RETURNS void\n" +
      "LANGUAGE plpgsql AS\n" +
      "$$\n" +
      "DECLARE row  record;\n" +
      "BEGIN\n" +
      "  FOR row IN\n" +
      "  SELECT\n" +
      "    table_schema,\n" +
      "    table_name\n" +
      "  FROM\n" +
      "    information_schema.tables\n" +
      "  WHERE\n" +
      "    table_name LIKE ('cia_migration%') or\n" +
      "    table_name LIKE ('frs_migration%')\n" +
      "  LOOP\n" +
      "    EXECUTE 'DROP TABLE ' || quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);\n" +
      "  END LOOP;\n" +
      "END;\n" +
      "$$;\n" +
      "\n" +
      "select removeTables()";

    try (Statement st = connection.createStatement()) {
      st.execute(sql);
    }
  }
}
