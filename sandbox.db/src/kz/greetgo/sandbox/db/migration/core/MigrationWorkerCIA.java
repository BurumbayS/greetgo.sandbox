package kz.greetgo.sandbox.db.migration.core;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;


public class MigrationWorkerCIA extends MigrationWorker {

  private InputStream inputStream;
  private OutputStream errorOutStream;

  private int batchSize = 0;

  private String tmpClientTable, tmpPhoneTable;

  public String r(String sql) {
    sql = sql.replaceAll("TMP_CLIENT", tmpClientTable);
    sql = sql.replaceAll("TMP_PHONE", tmpPhoneTable);
    return sql;
  }

  public MigrationWorkerCIA(Connection connection, InputStream inputStream, OutputStream errorOutStream, int batchSize) {
    this.connection = connection;
    this.inputStream = inputStream;
    this.errorOutStream = errorOutStream;
    this.batchSize = batchSize;

    tmpClientTable = "cia_migration_client_";
    tmpPhoneTable = "cia_migration_phone_";
  }

  public void migrate() throws Exception {

    createTmpTables();

    download();

    verification();

    migrateFromTmp();
  }

  public void createTmpTables() throws Exception {

    //language=PostgreSQL
    exec("CREATE TABLE TMP_CLIENT (\n" +
      "  client_id VARCHAR(20),\n" +
      "  status INT NOT NULL DEFAULT 0,\n" +
      "  error VARCHAR(300),\n" +
      "  line BIGINT,\n" +
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
      "  line BIGINT,\n" +
      "  \n" +
      "  num BIGSERIAL PRIMARY KEY,\n" +
      "  cia_id VARCHAR(100) NOT NULL,\n" +
      "  tmp_client_id VARCHAR(100) NOT NULL,\n" +
      "  number VARCHAR(100),\n" +
      "  phoneType VARCHAR(20),\n" +
      "  client_id VARCHAR(100)\n" +
      ")");
  }

  public void download() throws Exception {

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
    client_insert.field(15, "line", "?");

    Insert phone_insert = new Insert("TMP_PHONE");
    phone_insert.field(1, "cia_id", "?");
    phone_insert.field(2, "number", "?");
    phone_insert.field(3, "phoneType", "?");
    phone_insert.field(4, "tmp_client_id", "?");
    phone_insert.field(5, "line", "?");

    connection.setAutoCommit(false);

    try (PreparedStatement clientPS = connection.prepareStatement(r(client_insert.toString()))) {

      try (PreparedStatement phonePS = connection.prepareStatement(r(phone_insert.toString()))) {

        FromXMLParser fromXMLParser = new FromXMLParser();
        fromXMLParser.execute(connection, clientPS, phonePS, batchSize);

        recordsCount = fromXMLParser.parseRecordData(inputStream);

        if (fromXMLParser.getClientBatchSize() > 0 || fromXMLParser.getPhoneBatchSize() > 0) {
          phonePS.executeBatch();
          clientPS.executeBatch();
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
    exec("UPDATE TMP_PHONE ph SET status = 1" +
      " FROM TMP_CLIENT cl WHERE cl.id = ph.tmp_client_id AND cl.status = 1");

    uploadErrorsToFile();

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
  }

  private void uploadErrorsToFile() throws Exception {

    try (OutputStreamWriter writer = new OutputStreamWriter(errorOutStream)) {
      uploadErrors("TMP_CLIENT", writer);
    }

  }

  @SuppressWarnings("SqlResolve")
  public void migrateFromTmp() throws Exception {

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
  }
}
