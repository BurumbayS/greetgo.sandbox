package kz.greetgo.sandbox.db.migration.core;

import kz.greetgo.sandbox.db.migration.model.ClientXMLRecord;
import kz.greetgo.sandbox.db.migration.util.SaxHandler;
import kz.greetgo.util.RND;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class FromXMLParser extends SaxHandler {
  ClientXMLRecord clientXMLRecord;
  List<ClientXMLRecord> clientXMLRecords;

  Connection connection;
  PreparedStatement clientPS, phonePS;

  int clientBatchSize, phoneBatchSize;
  int MAX_BATCH_SIZE;
  int recordsCount;
  long curClientLine;
  long curLine = 0;

  public void execute(Connection connection, PreparedStatement clientPS, PreparedStatement phonePS, int batchSize) {
    this.connection = connection;
    this.clientPS = clientPS;
    this.phonePS = phonePS;
    this.MAX_BATCH_SIZE = batchSize;
    this.clientXMLRecords = new ArrayList<>();
  }

  public int parseRecordData(InputStream inputStream) throws SAXException, IOException {
    if (inputStream == null) return 0;

    clientXMLRecord = new ClientXMLRecord();

    XMLReader reader = XMLReaderFactory.createXMLReader();
    reader.setContentHandler(this);

    reader.parse(new InputSource(inputStream));

    return recordsCount;
  }

  public int getClientBatchSize() {
    return clientBatchSize;
  }

  public int getPhoneBatchSize() {
    return phoneBatchSize;
  }

  public List<ClientXMLRecord> getClientXMLRecords() { return clientXMLRecords; }

  @Override
  protected void startingTag(Attributes attributes) throws Exception {
    String path = path();
    if ("/client".equals(path) || "/cia/client".equals(path)) {
      curLine++;
      curClientLine = curLine;

      clientXMLRecord.cia_id = attributes.getValue("id");
      return;
    }
    if ("/client/name".equals(path) || "/cia/client/name".equals(path)) {
      curLine++;
      clientXMLRecord.name = attributes.getValue("value");
      return;
    }
    if ("/client/surname".equals(path) || "/cia/client/surname".equals(path)) {
      curLine++;
      clientXMLRecord.surname = attributes.getValue("value");
      return;
    }
    if ("/client/patronymic".equals(path) || "/cia/client/patronymic".equals(path)) {
      curLine++;
      clientXMLRecord.patronymic = attributes.getValue("value");
      return;
    }
    if ("/client/gender".equals(path) || "/cia/client/gender".equals(path)) {
      curLine++;
      clientXMLRecord.gender = attributes.getValue("value");
      return;
    }
    if ("/client/charm".equals(path) || "/cia/client/charm".equals(path)) {
      curLine++;
      clientXMLRecord.charm = attributes.getValue("value");
      return;
    }
    if ("/client/birth".equals(path) || "/cia/client/birth".equals(path)) {
      curLine++;
      try {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        clientXMLRecord.birthDate = new java.sql.Date(sdf.parse(attributes.getValue("value")).getTime());
      } catch (Exception e) {
//                e.printStackTrace();
      }
      return;
    }

    if ("/client/address/fact".equals(path) || "/cia/client/address/fact".equals(path)) {
      clientXMLRecord.fStreet = attributes.getValue("street");
      clientXMLRecord.fHouse = attributes.getValue("house");
      clientXMLRecord.fFlat = attributes.getValue("flat");
      return;
    }
    if ("/client/address/register".equals(path) || "/cia/client/address/register".equals(path)) {
      clientXMLRecord.rStreet = attributes.getValue("street");
      clientXMLRecord.rHouse = attributes.getValue("house");
      clientXMLRecord.rFlat = attributes.getValue("flat");
      return;
    }
  }

  @Override
  protected void endedTag(String tagName) throws Exception {
    String path = path() + "/" + tagName;

    if ("/client/homePhone".equals(path) || "/cia/client/homePhone".equals(path)) {
      if (!"".equals(text())) { clientXMLRecord.homePhones.add(text()); }
      return;
    }
    if ("/client/workPhone".equals(path) || "/cia/client/workPhone".equals(path)) {
      if (!"".equals(text())) { clientXMLRecord.workPhones.add(text()); }
      return;
    }
    if ("/client/mobilePhone".equals(path) || "/cia/client/mobilePhone".equals(path)) {
      if (!"".equals(text())) { clientXMLRecord.mobilePhones.add(text()); }
      return;
    }

    if ("/client/address".equals(path) || "/cia/client/address".equals(path)) {
      curLine += 4;
    }

    if ("/cia/client".equals(path)) {
      clientXMLRecord.id = RND.str(90);

      recordsCount++;
      clientXMLRecord.number = recordsCount;

      if (clientPS != null) {

        clientPS.setString(1, clientXMLRecord.cia_id);
        clientPS.setString(2, clientXMLRecord.surname);
        clientPS.setString(3, clientXMLRecord.name);
        clientPS.setString(4, clientXMLRecord.patronymic);
        clientPS.setString(5, clientXMLRecord.gender);
        clientPS.setString(6, clientXMLRecord.charm);
        clientPS.setDate(7, clientXMLRecord.birthDate);
        clientPS.setString(8, clientXMLRecord.id);
        clientPS.setString(9, clientXMLRecord.rStreet);
        clientPS.setString(10, clientXMLRecord.rHouse);
        clientPS.setString(11, clientXMLRecord.rFlat);
        clientPS.setString(12, clientXMLRecord.fStreet);
        clientPS.setString(13, clientXMLRecord.fHouse);
        clientPS.setString(14, clientXMLRecord.fFlat);
        clientPS.setLong(15, curClientLine);

        addPhones(clientXMLRecord.homePhones, "HOME");
        addPhones(clientXMLRecord.mobilePhones, "MOBILE");
        addPhones(clientXMLRecord.workPhones, "WORK");

        clientPS.addBatch();
        clientBatchSize++;

        if (clientBatchSize >= MAX_BATCH_SIZE) {
          clientPS.executeBatch();
          connection.commit();
          clientBatchSize = 0;
        }
      } else {
        clientXMLRecords.add(clientXMLRecord);
      }

      clientXMLRecord = new ClientXMLRecord();

      return;
    }
  }

  private void addPhones(List<String> phones, String phoneType) throws SQLException {
    for (String phone : phones) {
      phonePS.setString(1, clientXMLRecord.cia_id);
      phonePS.setString(2, phone);
      phonePS.setString(3, phoneType);
      phonePS.setString(4, clientXMLRecord.id);
      phonePS.setLong(5, curClientLine);

      phonePS.addBatch();
      phoneBatchSize++;

      if (phoneBatchSize > MAX_BATCH_SIZE) {
        phonePS.executeBatch();
        phoneBatchSize = 0;
      }
    }
  }
}
