package kz.greetgo.sandbox.db.migration;

import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.sandbox.controller.model.Client;
import kz.greetgo.sandbox.db.migration.core.FromJSONParser;
import kz.greetgo.sandbox.db.migration.core.FromXMLParser;
import kz.greetgo.sandbox.db.migration.core.Migration;
import kz.greetgo.sandbox.db.migration.model.AccountJSONRecord;
import kz.greetgo.sandbox.db.migration.model.ClientXMLRecord;
import kz.greetgo.sandbox.db.migration.model.TransactionJSONRecord;
import kz.greetgo.sandbox.db.test.dao.MigrationTestDao;
import kz.greetgo.sandbox.db.test.util.ParentTestNg;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

public class MigrationTest extends ParentTestNg {
  public BeanGetter<MigrationTestDao> migrationTestDao;

  Connection connection;
  Migration migration;


  @BeforeTest
  private void createConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    connection = DriverManager.getConnection(
      "jdbc:postgresql://localhost:5432/s_sandbox",
      "s_sandbox",
      "password"
    );

    List<String> frsFiles = new ArrayList<>();
    List<String> ciaFiles = new ArrayList<>();
    ciaFiles.add("build/out_files/from_cia_2018-02-21-154535-4-100000.xml");
    frsFiles.add("build/out_files/from_frs_2018-02-21-155113-2-700001.json_row.txt");
    migration = new Migration(connection, frsFiles, ciaFiles);
  }

  @Test
  public void TestTableCreation() throws Exception {
    this.deleteTables();

    //
    //
    migration.createTmpTables();
    //
    //

    List<String> tableNames = migrationTestDao.get().getCiaTableNames();
    assertThat(tableNames).hasSize(4);

    int cnt = 0;
    for (String table : tableNames) {
      if (table.contains("cia_migration_client")) cnt++;
      if (table.contains("cia_migration_phone")) cnt++;
      if (table.contains("frs_migration_account")) cnt++;
      if (table.contains("frs_migration_transaction")) cnt++;
    }
    assertThat(cnt).isEqualTo(4);
  }

  @Test
  public void TestDownloadFromCIA() throws Exception {

    this.clearCIATables();

    int expectedRecordsCount = 0;
    List<ClientXMLRecord> clientXMLRecords = null;

    try {
      File inputFile = new File("build/out_files/from_cia_2018-02-21-154535-4-100000.xml");

      FromXMLParser fromXMLParser = new FromXMLParser();
      fromXMLParser.execute(connection, null, null, 0);
//      expectedRecordsCount = fromXMLParser.parseRecordData(String.valueOf(inputFile));
      clientXMLRecords = fromXMLParser.getClientXMLRecords();

    } catch (Exception e) {
      e.printStackTrace();
    }

    //
    //
    int recordsCount = migration.downloadFromCIA("build/out_files/from_cia_2018-02-21-154535-4-100000.xml");
    //
    //

    assertThat(recordsCount).isEqualTo(expectedRecordsCount);

    Long cnt = 0L;
    for (ClientXMLRecord clientXMLRecord : clientXMLRecords) {
      cnt++;

      if (clientXMLRecord.name != null && clientXMLRecord.surname != null && clientXMLRecord.patronymic != null
          && clientXMLRecord.gender != null && clientXMLRecord.charm != null && clientXMLRecord.birthDate != null
          && clientXMLRecord.fStreet != null && clientXMLRecord.fHouse != null && clientXMLRecord.fFlat != null
          && clientXMLRecord.rStreet != null && clientXMLRecord.rHouse != null && clientXMLRecord.rFlat != null) {

        Integer clientsCount = migrationTestDao.get().getCiaClient(clientXMLRecord);
        assertThat(clientsCount).isNotZero();
      }

      String clientID = migrationTestDao.get().getCiaClientID(cnt);

      int numberCount = 0;
      for (String number : clientXMLRecord.mobilePhones) {
        if (migrationTestDao.get().getCiaPhone(clientXMLRecord.cia_id, number, "MOBILE") != null) {
          numberCount++;
        }
      }
      for (String number : clientXMLRecord.homePhones) {
        if (migrationTestDao.get().getCiaPhone(clientXMLRecord.cia_id, number, "HOME") != null) {
          numberCount++;
        }
      }
      for (String number : clientXMLRecord.workPhones) {
        if (migrationTestDao.get().getCiaPhone(clientXMLRecord.cia_id, number, "WORK") != null) {
          numberCount++;
        }
      }

      int expectedNumberCount = clientXMLRecord.mobilePhones.size() + clientXMLRecord.workPhones.size() + clientXMLRecord.homePhones.size();

      assertThat(clientID).isNotNull();
      assertThat(numberCount).isEqualTo(expectedNumberCount);
    }
  }

  @Test
  public void TestDownloadFromFRS() throws Exception {

    this.clearFRSTables();

    int expectedRecordsCount = 0;
    List<TransactionJSONRecord> transactionJSONRecords = null;
    List<AccountJSONRecord> accountJSONRecords = null;
    try {
      File inputFile = new File("build/out_files/from_frs_2018-02-21-155113-2-700001.json_row.txt");

      FromJSONParser fromJSONParser = new FromJSONParser();
      fromJSONParser.execute(connection, null, null, 0);
//      expectedRecordsCount = fromJSONParser.parseRecordData(inputFile);

      transactionJSONRecords = fromJSONParser.getTransactionJSONRecords();
      accountJSONRecords = fromJSONParser.getAccountJSONRecords();

    } catch (Exception e) {
      e.printStackTrace();
    }

    //
    //
    int recordsCount = migration.downloadFromFRS("build/out_files/from_frs_2018-02-21-155113-2-700001.json_row.txt");
    //
    //

    assertThat(recordsCount).isEqualTo(expectedRecordsCount);

    Long cnt = 0L;
    for (TransactionJSONRecord transactionJSONRecord : transactionJSONRecords) {
      cnt++;

      if (transactionJSONRecord.account_number != null && transactionJSONRecord.transaction_type != null
          && transactionJSONRecord.finished_at != null) {
        Integer transCount = migrationTestDao.get().getCiaTransaction(transactionJSONRecord);
        assertThat(transCount).isNotZero();
      }

      Long number = migrationTestDao.get().getTransactionCiaNumber(cnt);
      assertThat(number).isNotZero();
    }

    cnt = 0L;
    for (AccountJSONRecord accountJSONRecord : accountJSONRecords) {
      cnt++;

      if (accountJSONRecord.client_id != null && accountJSONRecord.account_number != null &&
          accountJSONRecord.registered_at != null) {
        Integer accountCount = migrationTestDao.get().getCiaAccount(accountJSONRecord);
        assertThat(accountCount).isNotZero();
      }

      Long number = migrationTestDao.get().getAccountCiaNumber(cnt);

      assertThat(number).isNotZero();
    }

  }

  @Test
  public void TestMigrateFromTmp() throws Exception {

    List<TransactionJSONRecord> transactionJSONRecords = null;
    List<AccountJSONRecord> accountJSONRecords = null;
    try {
      File inputFile = new File("build/out_files/from_frs_2018-05-24-095714-1-30005.json_row.txt");

      FromJSONParser fromJSONParser = new FromJSONParser();
      fromJSONParser.execute(connection, null, null, 0);
//      fromJSONParser.parseRecordData(inputFile);

      transactionJSONRecords = fromJSONParser.getTransactionJSONRecords();
      accountJSONRecords = fromJSONParser.getAccountJSONRecords();

    } catch (Exception e) {
      e.printStackTrace();
    }

    List<ClientXMLRecord> clientXMLRecords = null;

    try {
      File inputFile = new File("build/out_files/from_cia_2018-05-24-095644-2-3000.xml");

      FromXMLParser fromXMLParser = new FromXMLParser();
      fromXMLParser.execute(connection, null, null, 0);
//      fromXMLParser.parseRecordData(String.valueOf(inputFile));
      clientXMLRecords = fromXMLParser.getClientXMLRecords();

    } catch (Exception e) {
      e.printStackTrace();
    }

    //
    //
    migration.migrateFromTmp();
    //
    //

    int cnt = 0, length = clientXMLRecords.size();
    for (int i = 0; i < length; i++) {
      ClientXMLRecord clientXMLRecord = clientXMLRecords.get(cnt);

      if (clientXMLRecord.name == null || clientXMLRecord.surname == null || clientXMLRecord.birthDate == null ||
        clientXMLRecord.gender == null || clientXMLRecord.charm == null) {
        Integer status = migrationTestDao.get().getCiaClientStatus((long) i + 1);

        assertThat(status).isEqualTo(1);

        clientXMLRecords.remove(cnt);

        continue;
      }

      cnt++;
    }

    cnt = 0; length = accountJSONRecords.size();
    for (int i = 0; i < length; i++) {
      AccountJSONRecord accountJSONRecord = accountJSONRecords.get(cnt);

      if (accountJSONRecord.client_id == null || accountJSONRecord.account_number == null) {
        Integer status = migrationTestDao.get().getCiaAccountStatus((long) i + 1);
        assertThat(status).isEqualTo(1);

        accountJSONRecords.remove(cnt);

        continue;
      } else {
        String clientID = migrationTestDao.get().getClientID(accountJSONRecord.client_id);

        if (clientID == null) {
          Integer status = migrationTestDao.get().getCiaAccountStatus((long) i + 1);

          if (status == 0) {
            System.out.println("hello");
          }
          assertThat(status).isEqualTo(1);

          accountJSONRecords.remove(cnt);
        } else {
          cnt++;
        }
      }
    }

    cnt = 0; length = transactionJSONRecords.size();
    for (int i = 0; i < length; i++) {
      TransactionJSONRecord transactionJSONRecord = transactionJSONRecords.get(cnt);

      if (transactionJSONRecord.account_number == null || transactionJSONRecord.transaction_type == null) {
        Integer status = migrationTestDao.get().getCiaTransactionStatus((long) i + 1);
        assertThat(status).isEqualTo(1);

        transactionJSONRecords.remove(cnt);

        continue;
      } else {
        Integer accountID = migrationTestDao.get().getAccountID(transactionJSONRecord.account_number);

        if (accountID == null) {
          Integer status = migrationTestDao.get().getCiaTransactionStatus((long) i + 1);
          assertThat(status).isEqualTo(1);

          transactionJSONRecords.remove(cnt);
        } else {
          cnt++;
        }
      }
    }

    clientXMLRecords.sort(new Comparator<ClientXMLRecord>() {
      @Override
      public int compare(ClientXMLRecord o1, ClientXMLRecord o2) {
        if (o1.cia_id.equals(o2.cia_id)) {
          if (o1.number < o2.number) {
            return 1;
          } else { return -1; }
        } else { return o1.cia_id.compareTo(o2.cia_id); }
      }
    });

    int i = 1; length = clientXMLRecords.size();
    for (int j = 1; j < length; j++) {
      if (clientXMLRecords.get(i - 1).cia_id.equals(clientXMLRecords.get(i).cia_id)) {
        Integer status = migrationTestDao.get().getCiaClientStatus(clientXMLRecords.get(i).number);

        assertThat(status).isEqualTo(2);

        clientXMLRecords.remove(i);

      } else { i++; }
    }

    for(ClientXMLRecord clientXMLRecord : clientXMLRecords) {
      String clientID = migrationTestDao.get().getClient(clientXMLRecord);

      assertThat(clientID).isNotNull();

      for(String phone : clientXMLRecord.mobilePhones) {
        String phone_num = migrationTestDao.get().getPhone(phone, "MOBILE", clientXMLRecord.cia_id);

        assertThat(phone_num).isNotNull();
      }
      for(String phone : clientXMLRecord.homePhones) {
        String phone_num = migrationTestDao.get().getPhone(phone, "HOME", clientXMLRecord.cia_id);

        assertThat(phone_num).isNotNull();
      }
      for(String phone : clientXMLRecord.workPhones) {
        String phone_num = migrationTestDao.get().getPhone(phone, "WORK", clientXMLRecord.cia_id);

        assertThat(phone_num).isNotNull();
      }
    }

    for(AccountJSONRecord accountJSONRecord : accountJSONRecords) {
      String clientID = migrationTestDao.get().getAccountClientID(accountJSONRecord);
      String clientCiaID = migrationTestDao.get().getClientCiaID(clientID);

      assertThat(clientCiaID).isEqualTo(accountJSONRecord.client_id);
    }

    for(TransactionJSONRecord transactionJSONRecord : transactionJSONRecords) {
      Integer accountID = migrationTestDao.get().getTransactionAccountID(transactionJSONRecord);
      Integer transTypeID = migrationTestDao.get().getTransactionTypeID(transactionJSONRecord);

      String account = migrationTestDao.get().getAccountNumber(accountID);
      String transType = migrationTestDao.get().getTransactionTypeName(transTypeID);

      assertThat(account).isNotNull();
      assertThat(transType).isNotNull();
    }
  }

  private void clearCIATables() throws Exception {
    List<String> tableNames = migrationTestDao.get().getCiaTableNames();
    for (String table : tableNames) {
      if (table.contains("cia_migration_client") || table.contains("cia_migration_phone")) {
        String sql = "truncate " + table + " cascade";
        try (Statement st = connection.createStatement()) {
          st.execute(sql);
        }
      }
    }
  }

  private void clearFRSTables() throws Exception {
    List<String> tableNames = migrationTestDao.get().getCiaTableNames();
    for (String table : tableNames) {
      if (table.contains("frs_migration_account") || table.contains("frs_migration_transaction")) {
        String sql = "truncate " + table + " cascade";
        try (Statement st = connection.createStatement()) {
          st.execute(sql);
        }
      }
    }
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

  @AfterTest
  private void closeConnection() {
    if (this.connection != null) {
      try {
        this.connection.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      this.connection = null;
    }
  }
}
