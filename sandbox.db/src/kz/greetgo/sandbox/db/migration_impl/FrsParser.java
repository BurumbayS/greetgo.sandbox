package kz.greetgo.sandbox.db.migration_impl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import kz.greetgo.sandbox.db.migration_impl.model.Account;
import kz.greetgo.sandbox.db.migration_impl.model.Transaction;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.*;
import java.math.BigDecimal;
import java.util.function.Consumer;

public class FrsParser {

  private InputStream inputStream;
  private FrsTableWorker frsTableWorker;

  public FrsParser(TarArchiveInputStream inputStream, FrsTableWorker frsTableWorker) {
    this.inputStream = inputStream;
    this.frsTableWorker = frsTableWorker;
  }

  public static void main(String[] args) throws IOException {
    JsonParser jsonParser = new JsonFactory().createParser("{\"type\":\"new_account\",\"registered_at\":\"2001-03-01T10:30:22.547\",\"account_number\":\"49949KZ960-28847-33846-0544217\",\"client_id\":\"4-FPI-H3-SV-lsPFbXjtWC\"}");

    Account account = new Account();

    parseJSON(jsonParser, account);

    System.out.println("type: " + account.type + "\nclient id: " + account.clientId);

    jsonParser.close();


  }

  private static void parseJSON(JsonParser jsonParser, Account account) throws IOException {
    //loop through the JsonTokens
    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      String name = jsonParser.getCurrentName();
      String type;
      switch (name) {
        case "type":
          jsonParser.nextToken();
          type = jsonParser.getText();
          account.type = type;
          break;
        case "client_id":
          jsonParser.nextToken();
          account.clientId = jsonParser.getText();
          break;
        case "account_number":
          jsonParser.nextToken();
          account.accountNumber = jsonParser.getText();
          break;
        case "registered_at":
          jsonParser.nextToken();
          account.registeredAt = jsonParser.getText();
          break;
      }
    }
  }

  public int parseAndSave() throws IOException {
    int recordsCount = 0;
    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    while ((line = br.readLine()) != null) {
      Account account = new Account();
      Transaction transaction = new Transaction();

      JsonParser jsonParser = new JsonFactory().createParser(line);

      //Skip START_OBJECT
      jsonParser.nextToken();

      //loop through the JsonTokens
      String type = null;
      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        String name = jsonParser.getCurrentName();
        switch (name.trim()) {
          case "type":
            jsonParser.nextToken();
            type = jsonParser.getText();
            account.type = type;
            transaction.type = type;
            break;
          case "client_id":
            jsonParser.nextToken();
            account.clientId = jsonParser.getText();
            break;
          case "account_number":
            jsonParser.nextToken();
            account.accountNumber = jsonParser.getText();
            transaction.accountNumber = jsonParser.getText();
            break;
          case "registered_at":
            jsonParser.nextToken();
            account.registeredAt = jsonParser.getText();
            break;
          case "money":
            jsonParser.nextToken();
//            transaction.money = Double.parseDouble(jsonParser.getText().replace("_", ""));
            transaction.money = new BigDecimal(jsonParser.getText().replace("_", ""));
            break;
          case "finished_at":
            jsonParser.nextToken();
            transaction.finishedAt = jsonParser.getText();
            break;
          case "transaction_type":
            jsonParser.nextToken();
            transaction.transactionType = jsonParser.getText();
            break;
        }
      }
      if ("transaction".equals(type)) sendTo(frsTableWorker::addToBatch, transaction);
      else if ("new_account".equals(type)) sendTo(frsTableWorker::addToBatch, account);
      jsonParser.close();
      recordsCount++;
    }
    frsTableWorker.execBatch.run();
    return recordsCount;
  }

  private void sendTo(Consumer<Account> func, Account account) {
    func.accept(account);
  }

  private void sendTo(Consumer<Transaction> func, Transaction transaction) {
    func.accept(transaction);
  }

}