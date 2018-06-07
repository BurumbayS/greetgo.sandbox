package kz.greetgo.sandbox.db.migration.core;

import kz.greetgo.sandbox.db.migration.model.AccountJSONRecord;
import kz.greetgo.sandbox.db.migration.model.TransactionJSONRecord;
import net.sf.cglib.core.Local;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class FromJSONParser {
  PreparedStatement accPS, transPS;
  Connection connection;

  int accBatchSize, transBatchSize;
  int MAX_BATCH_SIZE;
  int recordsCount = 0;
  long curLine = 0;

  TransactionJSONRecord transactionJSONRecord;
  AccountJSONRecord accountJSONRecord;
  List<TransactionJSONRecord> transactionJSONRecords;
  List<AccountJSONRecord> accountJSONRecords;

  public void execute(Connection connection, PreparedStatement accPS, PreparedStatement transPS, int maxBatchSize) {
    this.accPS = accPS;
    this.transPS = transPS;
    this.MAX_BATCH_SIZE = maxBatchSize;
    this.connection = connection;

    transactionJSONRecords = new ArrayList<>();
    accountJSONRecords = new ArrayList<>();
  }

  public int parseRecordData(InputStream inputStream) throws Exception {

    transactionJSONRecord = new TransactionJSONRecord();
    accountJSONRecord = new AccountJSONRecord();

    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {

      for (String line; (line = br.readLine()) != null; ) {
        JSONObject obj = new JSONObject(line);

        if ("transaction".equals(obj.getString("type"))) {
          curLine++;

          String money = obj.getString("money");
          money = money.replaceAll("_", "");
          transactionJSONRecord.money = Float.valueOf(money);
          transactionJSONRecord.account_number = obj.getString("account_number");
          String tstmp = obj.getString("finished_at");
          //TODO: в этом параметре хранится дата в международном формате ISO
          //помимо Т в конце могут добавлятся другие символы относительно локали (региона)
          //поэтому это не верное решение, просто заменять символ Т. Вот пример 2011-08-12T20:17:46.384Z
          //Z - 'zero time zone' or 'Zulu time'
          //Найди другое решение для парсинга времени
          tstmp = tstmp.replaceAll("T", " ");
          transactionJSONRecord.finished_at = Timestamp.valueOf(tstmp);
          transactionJSONRecord.transaction_type = obj.getString("transaction_type");

          if (transPS != null) {
            addToTransPS(transactionJSONRecord);
          } else { transactionJSONRecords.add(transactionJSONRecord); }

          transactionJSONRecord = new TransactionJSONRecord();

        } else if ("new_account".equals(obj.getString("type"))) {
          curLine++;

          accountJSONRecord.account_number = obj.getString("account_number");
          String tstmp = obj.getString("registered_at");
          //TODO: в этом параметре хранится дата в международном формате ISO
          //помимо Т в конце могут добавлятся другие символы относительно локали (региона)
          //поэтому это не верное решение, просто заменять символ Т. Вот пример 2011-08-12T20:17:46.384Z
          //Z - 'zero time zone' or 'Zulu time'
          //Найди другое решение для парсинга времени

          LocalDateTime dateTime = LocalDateTime.parse(tstmp);

          tstmp = tstmp.replaceAll("T", " ");
          accountJSONRecord.registered_at = Timestamp.valueOf(dateTime);
          accountJSONRecord.client_id = obj.getString("client_id");

          if (accPS != null) {
            addToAccPS(accountJSONRecord);
          } else { accountJSONRecords.add(accountJSONRecord); }

          accountJSONRecord = new AccountJSONRecord();
        }

        recordsCount++;

        if (transPS != null && accPS != null) {
          if (transBatchSize > MAX_BATCH_SIZE || accBatchSize > MAX_BATCH_SIZE) {
            transPS.executeBatch();
            accPS.executeBatch();
            connection.commit();

            transBatchSize = 0;
            accBatchSize = 0;
          }
        }
      }
    }

    return recordsCount;
  }

  private void addToTransPS(TransactionJSONRecord transactionJSONRecord) throws Exception {
    this.transPS.setFloat(1, transactionJSONRecord.money);
    this.transPS.setString(2, transactionJSONRecord.account_number);
    this.transPS.setTimestamp(3, transactionJSONRecord.finished_at);
    this.transPS.setString(4, transactionJSONRecord.transaction_type);
    this.transPS.setLong(5, curLine);

    transPS.addBatch();
    transBatchSize++;
  }

  private void addToAccPS(AccountJSONRecord accountJSONRecord) throws Exception {

    this.accPS.setString(1, accountJSONRecord.account_number);
    this.accPS.setTimestamp(2, accountJSONRecord.registered_at);
    this.accPS.setString(3, accountJSONRecord.client_id);
    this.accPS.setLong(4, curLine);

    accPS.addBatch();
    accBatchSize++;
  }

  public int getAccBatchSize() { return transBatchSize; }

  public int getTransBatchSize() { return accBatchSize; }

  public List<TransactionJSONRecord> getTransactionJSONRecords() { return transactionJSONRecords; }

  public List<AccountJSONRecord> getAccountJSONRecords() { return accountJSONRecords; }

}
