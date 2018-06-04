package kz.greetgo.sandbox.db.register_impl;

import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.sandbox.controller.model.*;
import kz.greetgo.sandbox.controller.register.ClientRegister;
import kz.greetgo.sandbox.controller.report.ClientsListReportView;
import kz.greetgo.sandbox.controller.report.model.ClientListRow;
import kz.greetgo.sandbox.db.model.Sorter;
import kz.greetgo.sandbox.db.stand.beans.StandDb;
import kz.greetgo.sandbox.db.stand.model.AccountDot;
import kz.greetgo.sandbox.db.stand.model.AdressDot;
import kz.greetgo.sandbox.db.stand.model.CharmDot;
import kz.greetgo.sandbox.db.stand.model.ClientDot;
import kz.greetgo.sandbox.db.stand.model.PhoneDot;
import kz.greetgo.sandbox.db.test.dao.AccountTestDao;
import kz.greetgo.sandbox.db.test.dao.AdressTestDao;
import kz.greetgo.sandbox.db.test.dao.CharmTestDao;
import kz.greetgo.sandbox.db.test.dao.ClientTestDao;
import kz.greetgo.sandbox.db.test.dao.PhoneTestDao;
import kz.greetgo.sandbox.db.test.dao.ReportParamsDao;
import kz.greetgo.sandbox.db.test.util.ParentTestNg;
import kz.greetgo.util.RND;
import org.fest.assertions.api.Assertions;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

public class ClientRegisterImplTest extends ParentTestNg {

  public BeanGetter<ClientRegister> clientRegister;
  public BeanGetter<CharmTestDao> charmTestDao;
  public BeanGetter<ClientTestDao> clientTestDao;
  public BeanGetter<AccountTestDao> accountTestDao;
  public BeanGetter<PhoneTestDao> phoneTestDao;
  public BeanGetter<AdressTestDao> adressTestDao;
  public BeanGetter<ReportParamsDao> reportParamsDao;
  public BeanGetter<StandDb> standDb;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test(expectedExceptions = RuntimeException.class)
  public void testEditFormFill() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    adressTestDao.get().clearAdresses();
    phoneTestDao.get().clearPhones();

    CharmDot charmDot = new CharmDot();
    charmDot.id = 1;
    charmDot.name = "меланхолик";
    charmDot.description = "asdasd";
    charmDot.energy = (float) 123;
    charmTestDao.get().insertCharm(charmDot);

    ClientToSave clientToSave = new ClientToSave();
    clientToSave.id = 1;
    clientToSave.name = null;
    clientToSave.surname = null;
    clientToSave.gender = "MALE";
    clientToSave.birth_date = "1997-09-27";
    clientToSave.charm_id = 1;
    clientToSave.rAdressStreet = "Каратал";
    clientToSave.rAdressHouse = "15";
    clientToSave.rAdressFlat = "25";

    //
    //
    ClientRecord clientRecord = clientRegister.get().addNewClient(clientToSave);
    //
    //

//    assertThat(false).isTrue();
    //TODO есть специально для такого случая что-то типа такого
    //Done
    Assertions.fail("Incorrect form filling");
  }

  @Test
  //TODO: исправить тест
  //Done
  public void testAddNewClient() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    adressTestDao.get().clearAdresses();
    phoneTestDao.get().clearPhones();

    CharmDot charmDot = new CharmDot();
    charmDot.id = 1;
    charmDot.name = "меланхолик";
    charmDot.description = "asdasd";
    charmDot.energy = (float) 123;
    charmTestDao.get().insertCharm(charmDot);

    ClientToSave clientToSave = new ClientToSave();
    clientToSave.id = 1;
    clientToSave.name = "Владимир";
    clientToSave.surname = "Путин";
    clientToSave.patronymic = "Владимирович";
    clientToSave.gender = "MALE";
    clientToSave.birth_date = "1997-09-27";
    clientToSave.charm_id = 1;
    clientToSave.mobilePhones.add("87779105332");
    clientToSave.rAdressStreet = "Каратал";
    clientToSave.rAdressHouse = "15";
    clientToSave.rAdressFlat = "25";

    //
    //
    ClientRecord clientRecord = clientRegister.get().addNewClient(clientToSave);
    //
    //

    List<Adress> adresses = adressTestDao.get().getAdress(clientRecord.id);
    List<Phone> phones = phoneTestDao.get().getPhones(clientRecord.id);

    assertThat(clientRecord).isNotNull();
    String fio = clientToSave.surname + " " + clientToSave.name + " " + clientToSave.patronymic;
    assertThat(clientRecord.fio).isEqualTo(fio);
    assertThat(clientRecord.charm).isEqualTo(charmDot.name);
    assertThat(clientRecord.age).isEqualTo(20);
    assertThat(adresses).isNotNull();
    assertThat(adresses).hasSize(1);
    assertThat(adresses.get(0).adressType).isEqualTo("REG");
    assertThat(adresses.get(0).street).isEqualTo(clientToSave.rAdressStreet);
    assertThat(phones).isNotNull();
    assertThat(phones).hasSize(1);
    assertThat(phones.get(0).number).isEqualTo(clientToSave.mobilePhones.get(0));

    List<ClientDot> clients = clientTestDao.get().getAllClients();
    assertThat(clients.get(0).surname).isEqualTo(clientToSave.surname);
    assertThat(clients.get(0).name).isEqualTo(clientToSave.name);
    assertThat(clients.get(0).patronymic).isEqualTo(clientToSave.patronymic);
    assertThat(clients.get(0).birth_date).isEqualTo(clientToSave.birth_date);
    assertThat(clients.get(0).charm_id).isEqualTo(clientToSave.charm_id);
    assertThat(clients.get(0).gender).isEqualTo(clientToSave.gender);
  }

  @Test
  public void testUpdateClient() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    phoneTestDao.get().clearPhones();
    adressTestDao.get().clearAdresses();

    CharmDot charmDot = new CharmDot();
    charmDot.id = 1;
    charmDot.name = "меланхолик";
    charmDot.description = "asdasd";
    charmDot.energy = (float) 123;
    charmTestDao.get().insertCharm(charmDot);

    ClientToSave clientToSave = new ClientToSave();
    clientToSave.name = "Владимир";
    clientToSave.surname = "Путин";
    clientToSave.patronymic = "Владимирович";
    clientToSave.gender = "MALE";
    clientToSave.birth_date = "1997-09-27";
    clientToSave.charm_id = 1;
    clientToSave.mobilePhones.add("87779105332");
    clientToSave.rAdressStreet = "Каратал";
    clientToSave.rAdressHouse = "15";
    clientToSave.rAdressFlat = "25";
    ClientRecord clientRecord1 = clientRegister.get().addNewClient(clientToSave);

    clientToSave.id = clientRecord1.id;
    clientToSave.name = "Александр";
    clientToSave.surname = "Пушкин";
    clientToSave.patronymic = "Сергеевич";
    clientToSave.gender = "MALE";
    clientToSave.birth_date = "1987-09-27";
    clientToSave.charm_id = 1;
    clientToSave.mobilePhones.add("87474415332");
    clientToSave.rAdressStreet = "Каратал";
    clientToSave.rAdressHouse = "15";
    clientToSave.rAdressFlat = "25";
    clientToSave.fAdressStreet = "Кабанбай батыра";
    clientToSave.fAdressHouse = "138";
    clientToSave.fAdressFlat = "9";
    clientToSave.homePhone.add("87282305227");

    //
    //
    ClientRecord clientRecord2 = clientRegister.get().updateClient(clientToSave);
    //
    //

    List<Adress> adresses = adressTestDao.get().getAdress(clientRecord1.id);
    List<Phone> phones = phoneTestDao.get().getPhones(clientRecord1.id);

    assertThat(clientRecord2).isNotNull();
    assertThat(clientRecord2.id).isEqualTo(clientRecord1.id);
    String fio = clientToSave.surname + " " + clientToSave.name + " " + clientToSave.patronymic;
    assertThat(clientRecord2.fio).isEqualTo(fio);
    assertThat(clientRecord2.charm).isEqualTo(charmDot.name);
    assertThat(clientRecord2.age).isEqualTo(30);
    assertThat(adresses).isNotNull();
    assertThat(adresses).hasSize(2);
    assertThat(adresses.get(0).adressType).isEqualTo("REG");
    assertThat(adresses.get(0).street).isEqualTo(clientToSave.rAdressStreet);
    assertThat(adresses.get(1).adressType).isEqualTo("FACT");
    assertThat(adresses.get(1).street).isEqualTo(clientToSave.fAdressStreet);
    assertThat(phones).isNotNull();
    assertThat(phones).hasSize(3);
    assertThat(phones.get(0).number).isEqualTo(clientToSave.mobilePhones.get(0));
    assertThat(phones.get(1).number).isEqualTo(clientToSave.mobilePhones.get(1));
    assertThat(phones.get(2).number).isEqualTo(clientToSave.homePhone.get(0));
  }

  @Test
  public void testRemoveClient() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();

    CharmDot charmDot = new CharmDot();
    charmDot.id = 1;
    charmDot.name = "меланхолик";
    charmDot.description = "asdasd";
    charmDot.energy = (float) 123;
    charmTestDao.get().insertCharm(charmDot);

    ClientDot clientDot = new ClientDot();
    clientDot.id = 1;
    clientDot.name = "Владимир";
    clientDot.surname = "Путин";
    clientDot.patronymic = "Владимирович";
    clientDot.gender = "MALE";
    clientDot.birth_date = new SimpleDateFormat("yyyyMMdd").parse("20100520");
    clientDot.charm_id = 1;
    clientTestDao.get().insertClient(clientDot);

    //
    //
    String str = clientRegister.get().removeClient(String.valueOf(1));
    //
    //

    List<ClientDot> clients = clientTestDao.get().getAllClients();

    assertThat(str).isEqualTo(String.valueOf(1));
    assertThat(clients).hasSize(0);
  }

  @Test//TODO имя теста тоже надо корректировать
  //Done
  public void testGetClientDetails() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    adressTestDao.get().clearAdresses();
    phoneTestDao.get().clearPhones();

    CharmDot charmDot = new CharmDot();
    charmDot.id = 1;
    charmDot.name = "меланхолик";
    charmDot.description = "asdasd";
    charmDot.energy = (float) 123;
    charmTestDao.get().insertCharm(charmDot);

    ClientDot clientDot = new ClientDot();
    clientDot.id = 1;
    clientDot.name = "Владимир";
    clientDot.surname = "Путин";
    clientDot.patronymic = "Владимирович";
    clientDot.gender = "MALE";
    clientDot.birth_date = new SimpleDateFormat("yyyyMMdd").parse("20100520");
    clientDot.charm_id = 1;
    clientTestDao.get().insertClient(clientDot);

    PhoneDot phoneDot = new PhoneDot();
    phoneDot.clientID = 1;
    phoneDot.number = "87779105332";
    phoneDot.phoneType = "MOBILE";
    phoneTestDao.get().insertPhone(phoneDot);

    AdressDot adressDot = new AdressDot();
    adressDot.id = 1;
    adressDot.clientID = 1;
    adressDot.street = "Каратал";
    adressDot.house = "15";
    adressDot.flat = "25";
    adressDot.adressType = "REG";
    adressTestDao.get().insertAdress(adressDot);

    //
    //
    ClientDetails clientDetails = clientRegister.get().getClientDetails("1");
    //
    //

    assertThat(clientDetails).isNotNull();
    assertThat(clientDetails.id).isEqualTo(clientDot.id);
    assertThat(clientDetails.name).isEqualTo(clientDot.name);
    assertThat(clientDetails.surname).isEqualTo(clientDot.surname);
    assertThat(clientDetails.patronymic).isEqualTo(clientDot.patronymic);
    assertThat(clientDetails.gender).isEqualTo(clientDot.gender);
    assertThat(clientDetails.charm_id).isEqualTo(charmDot.id);
    assertThat(clientDetails.mobilePhones.get(0)).isEqualTo(phoneDot.number);
    assertThat(clientDetails.rAdressStreet).isEqualTo(adressDot.street);
  }

  @Test
  public void testMiddlePageSortedByAgeUp() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    accountTestDao.get().clearAccounts();

    List<CharmDot> charms = genCharms();

    List<ClientDot> clientDots = genClients();

    clientDots.sort(new Comparator<ClientDot>() {
      @Override
      public int compare(ClientDot o1, ClientDot o2) {
        if (o1.countAge() > o2.countAge()) {
          return 1;
        } else if (o1.countAge() < o2.countAge()) {
          return -1;
        } else {return 0;}
      }
    });

    FilterSortParams filterSortParams = new FilterSortParams();
    filterSortParams.filterStr = "";
    filterSortParams.sortBy = "age";
    filterSortParams.sortOrder = "up";
    ClientsListParams clientsListParams = new ClientsListParams();
    clientsListParams.pageID = 5;
    clientsListParams.filterSortParams = filterSortParams;

    //
    //
    ClientToReturn clients = clientRegister.get().getClientsRecordList(clientsListParams);
    //
    //

    assertThat(clients).isNotNull();
    assertThat(clients.clientInfos).isNotNull();
    assertThat(clients.clientInfos).hasSize(3);
    for (int i = 0; i < 3; i++) {
      assertThat(clients.clientInfos.get(i).id).isEqualTo(clientDots.get(i + 12).id);
      assertThat(clients.clientInfos.get(i).age).isEqualTo(clientDots.get(i + 12).countAge());
      String fio = clientDots.get(i + 12).surname + " " + clientDots.get(i + 12).name + " " + clientDots.get(i + 12).patronymic;
      assertThat(clients.clientInfos.get(i).fio).isEqualTo(fio);
    }
  }

  //TODO: нет сортировки по убыванию
  //Done
  @Test
  public void testMiddlePageSortedByAgeDown() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    accountTestDao.get().clearAccounts();

    //TODO тесты не должны зависеть от стэнда
    //Done
    List<CharmDot> charms = genCharms();

    List<ClientDot> clientDots = genClients();

    clientDots.sort(new Comparator<ClientDot>() {
      @Override
      public int compare(ClientDot o1, ClientDot o2) {
        if (o1.countAge() < o2.countAge()) {
          return 1;
        } else if (o1.countAge() > o2.countAge()) {
          return -1;
        } else {return 0;}
      }
    });

    FilterSortParams filterSortParams = new FilterSortParams();
    filterSortParams.filterStr = "";
    filterSortParams.sortBy = "age";
    filterSortParams.sortOrder = "down";
    ClientsListParams clientsListParams = new ClientsListParams();
    clientsListParams.pageID = 5;
    clientsListParams.filterSortParams = filterSortParams;

    //
    //
    ClientToReturn clients = clientRegister.get().getClientsRecordList(clientsListParams);
    //
    //

    assertThat(clients).isNotNull();
    assertThat(clients.clientInfos).isNotNull();
    assertThat(clients.clientInfos).hasSize(3);
    for (int i = 0; i < 3; i++) {
      assertThat(clients.clientInfos.get(i).id).isEqualTo(clientDots.get(i + 12).id);
      assertThat(clients.clientInfos.get(i).age).isEqualTo(clientDots.get(i + 12).countAge());
      String fio = clientDots.get(i + 12).surname + " " + clientDots.get(i + 12).name + " " + clientDots.get(i + 12).patronymic;
      assertThat(clients.clientInfos.get(i).fio).isEqualTo(fio);
    }
  }

  @Test
  public void testLastPageSortedByAgeUp() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    accountTestDao.get().clearAccounts();

    List<CharmDot> charms = genCharms();

    List<ClientDot> clientDots = genClients();

    clientDots.sort(new Comparator<ClientDot>() {
      @Override
      public int compare(ClientDot o1, ClientDot o2) {
        if (o1.countAge() > o2.countAge()) {
          return 1;
        } else if (o1.countAge() < o2.countAge()) {
          return -1;
        } else {return 0;}
      }
    });

    FilterSortParams filterSortParams = new FilterSortParams();
    filterSortParams.filterStr = "";
    filterSortParams.sortBy = "age";
    filterSortParams.sortOrder = "up";
    ClientsListParams clientsListParams = new ClientsListParams();
    clientsListParams.pageID = 10;
    clientsListParams.filterSortParams = filterSortParams;

    //
    //
    ClientToReturn clients = clientRegister.get().getClientsRecordList(clientsListParams);
    //
    //

    assertThat(clients).isNotNull();
    assertThat(clients.clientInfos).isNotNull();
    assertThat(clients.clientInfos).hasSize(3);
    for (int i = 0; i < 3; i++) {
      assertThat(clients.clientInfos.get(i).id).isEqualTo(clientDots.get(i + 27).id);
      assertThat(clients.clientInfos.get(i).age).isEqualTo(clientDots.get(i + 27).countAge());
      String fio = clientDots.get(i + 27).surname + " " + clientDots.get(i + 27).name + " " + clientDots.get(i + 27).patronymic;
      assertThat(clients.clientInfos.get(i).fio).isEqualTo(fio);
    }
  }

  @Test
  public void testLastPageSortedByAgeDown() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    accountTestDao.get().clearAccounts();

    List<CharmDot> charms = genCharms();

    List<ClientDot> clientDots = genClients();

    clientDots.sort((o1, o2) -> {
      //noinspection Duplicates
      return Integer.compare(o2.countAge(), o1.countAge());
    });

    FilterSortParams filterSortParams = new FilterSortParams();
    filterSortParams.filterStr = "";
    filterSortParams.sortBy = "age";
    filterSortParams.sortOrder = "down";
    ClientsListParams clientsListParams = new ClientsListParams();
    clientsListParams.pageID = 10;
    clientsListParams.filterSortParams = filterSortParams;

    //
    //
    ClientToReturn clients = clientRegister.get().getClientsRecordList(clientsListParams);
    //
    //

    assertThat(clients).isNotNull();
    assertThat(clients.clientInfos).isNotNull();
    assertThat(clients.clientInfos).hasSize(3);
    //noinspection Duplicates
    for (int i = 0; i < 3; i++) {
      assertThat(clients.clientInfos.get(i).id).isEqualTo(clientDots.get(i + 27).id);
      assertThat(clients.clientInfos.get(i).age).isEqualTo(clientDots.get(i + 27).countAge());
      String fio = clientDots.get(i + 27).surname + " " + clientDots.get(i + 27).name + " " + clientDots.get(i + 27).patronymic;
      assertThat(clients.clientInfos.get(i).fio).isEqualTo(fio);
    }
  }

  @Test
  public void testGetFilteredClientsInfo() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    accountTestDao.get().clearAccounts();

    List<CharmDot> charms = genCharms();

    List<ClientDot> clientDots = genClients();

    ClientDot clientDot = clientDots.get(0);
    ClientRecord expectedClientRecord = new ClientRecord();
    expectedClientRecord.fio = clientDot.surname + " " + clientDot.name + " " + clientDot.patronymic;
    expectedClientRecord.id = 1;
    expectedClientRecord.charm = charms.get(clientDot.charm_id - 1).name;
    expectedClientRecord.age = clientDot.countAge();

    expectedClientRecord.minCash = 2000000;
    expectedClientRecord.maxCash = 0;
    expectedClientRecord.totalCash = 0;
    for (int i = 0; i < 5; i++) {
      AccountDot accountDot = new AccountDot();
      accountDot.id = i + 1;
      accountDot.money = (float) RND.plusInt(100000);
      accountDot.number = RND.str(10);
      accountDot.registered_at = Timestamp.valueOf(LocalDateTime.now());
      accountDot.clientID = 1;
      accountTestDao.get().insertAccount(accountDot);

      if (accountDot.money < expectedClientRecord.minCash) { expectedClientRecord.minCash = accountDot.money; }
      if (accountDot.money > expectedClientRecord.maxCash) { expectedClientRecord.maxCash = accountDot.money; }
      expectedClientRecord.totalCash += accountDot.money;
    }

    TestView view = new TestView();
    FilterSortParams filterSortParams = new FilterSortParams();
    filterSortParams.filterStr = clientDot.name;
    filterSortParams.sortBy = "";
    filterSortParams.sortOrder = "";
    ClientsListParams clientsListParams = new ClientsListParams();
    clientsListParams.pageID = 1;
    clientsListParams.filterSortParams = filterSortParams;
    ClientsListReportParams clientsListReportParams = new ClientsListReportParams("Пушкин", view, filterSortParams);

    //
    //
    ClientToReturn clients = clientRegister.get().getClientsRecordList(clientsListParams);
    clientRegister.get().genClientListReport(clientsListReportParams);
    //
    //

    assertThat(clients).isNotNull();
    assertThat(clients.pageCount).isEqualTo(1);
    assertThat(clients.clientInfos).isNotNull();
    assertThat(clients.clientInfos).hasSize(1);
    assertThat(clients.clientInfos.get(0).id).isEqualTo(expectedClientRecord.id);
    assertThat(clients.clientInfos.get(0).fio).isEqualTo(expectedClientRecord.fio);
    assertThat(clients.clientInfos.get(0).age).isEqualTo(expectedClientRecord.age);
    assertThat(clients.clientInfos.get(0).totalCash).isEqualTo(expectedClientRecord.totalCash);
    assertThat(clients.clientInfos.get(0).minCash).isEqualTo(expectedClientRecord.minCash);
    assertThat(clients.clientInfos.get(0).maxCash).isEqualTo(expectedClientRecord.maxCash);

    assertThat(view.rowList).hasSize(1);
    assertThat(view.rowList.get(0).fio).isEqualTo(expectedClientRecord.fio);
  }

  @Test
  public void testGetFilteredClientsInfoSortedByFIOUp() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    accountTestDao.get().clearAccounts();

    List<CharmDot> charms = genCharms();

    List<ClientDot> clientDots = genClients();
    List<ClientRecord> expectedClients = getClientRecordFromDot(clientDots);

    TestView view = new TestView();

    FilterSortParams filterSortParams = new FilterSortParams();
    filterSortParams.filterStr = "";
    filterSortParams.sortBy = "fio";
    filterSortParams.sortOrder = "up";
    ClientsListParams clientsListParams = new ClientsListParams();
    clientsListParams.pageID = 1;
    clientsListParams.filterSortParams = filterSortParams;
    ClientsListReportParams clientsListReportParams = new ClientsListReportParams("Пушкин", view, filterSortParams);

    //
    //
    ClientToReturn clients = clientRegister.get().getClientsRecordList(clientsListParams);
    clientRegister.get().genClientListReport(clientsListReportParams);
    //
    //

    expectedClients.sort(new Comparator<ClientRecord>() {
      @Override
      public int compare(ClientRecord o1, ClientRecord o2) {
        return o1.fio.compareTo(o2.fio);
      }
    });

    assertThat(clients).isNotNull();
    assertThat(clients.pageCount).isEqualTo(10);
    assertThat(clients.clientInfos).isNotNull();
    assertThat(clients.clientInfos).hasSize(3);
    for(int i = 0; i < 3; i++) {
      assertThat(clients.clientInfos.get(i).id).isEqualTo(expectedClients.get(i).id);
      assertThat(clients.clientInfos.get(i).fio).isEqualTo(expectedClients.get(i).fio);
    }

    assertThat(view.rowList).hasSize(expectedClients.size());
    for(int i = 0; i < expectedClients.size(); i++) {
      assertThat(view.rowList.get(i).fio).isEqualTo(expectedClients.get(i).fio);
    }
  }

  @Test
  public void testGetFilteredClientsInfoSortedByFIODown() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    accountTestDao.get().clearAccounts();

    List<CharmDot> charms = genCharms();

    List<ClientDot> clientDots = genClients();
    List<ClientRecord> expectedClients = getClientRecordFromDot(clientDots);

    TestView view = new TestView();

    FilterSortParams filterSortParams = new FilterSortParams();
    filterSortParams.filterStr = "";
    filterSortParams.sortBy = "fio";
    filterSortParams.sortOrder = "down";
    ClientsListParams clientsListParams = new ClientsListParams();
    clientsListParams.pageID = 1;
    clientsListParams.filterSortParams = filterSortParams;
    ClientsListReportParams clientsListReportParams = new ClientsListReportParams("Пушкин", view, filterSortParams);

    //
    //
    ClientToReturn clients = clientRegister.get().getClientsRecordList(clientsListParams);
    clientRegister.get().genClientListReport(clientsListReportParams);
    //
    //

    expectedClients.sort(new Comparator<ClientRecord>() {
      @Override
      public int compare(ClientRecord o1, ClientRecord o2) {
        return o2.fio.compareTo(o1.fio);
      }
    });

    assertThat(clients).isNotNull();
    assertThat(clients.pageCount).isEqualTo(10);
    assertThat(clients.clientInfos).isNotNull();
    assertThat(clients.clientInfos).hasSize(3);
    for(int i = 0; i < 3; i++) {
      assertThat(clients.clientInfos.get(i).id).isEqualTo(expectedClients.get(i).id);
      assertThat(clients.clientInfos.get(i).fio).isEqualTo(expectedClients.get(i).fio);
    }

    assertThat(view.rowList).hasSize(expectedClients.size());
    for(int i = 0; i < expectedClients.size(); i++) {
      assertThat(view.rowList.get(i).fio).isEqualTo(expectedClients.get(i).fio);
    }
  }

  @Test
  public void testGetFilteredClientsInfoSortedByAgeUp() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    accountTestDao.get().clearAccounts();

    List<CharmDot> ch = genCharms();
    List<ClientDot> clientDots = genClients();
    List<ClientRecord> expectedClients = getClientRecordFromDot(clientDots);

    TestView view = new TestView();

    FilterSortParams filterSortParams = new FilterSortParams();
    filterSortParams.filterStr = "";
    filterSortParams.sortBy = "age";
    filterSortParams.sortOrder = "up";
    ClientsListParams clientsListParams = new ClientsListParams();
    clientsListParams.pageID = 1;
    clientsListParams.filterSortParams = filterSortParams;
    ClientsListReportParams clientsListReportParams = new ClientsListReportParams("Пушкин", view, filterSortParams);

    //
    //
    ClientToReturn clients = clientRegister.get().getClientsRecordList(clientsListParams);
    clientRegister.get().genClientListReport(clientsListReportParams);
    //
    //

    expectedClients.sort(new Comparator<ClientRecord>() {
      @Override
      public int compare(ClientRecord o1, ClientRecord o2) {
        return Integer.compare(o1.age, o2.age);
      }
    });

    assertThat(clients).isNotNull();
    assertThat(clients.pageCount).isEqualTo(10);
    assertThat(clients.clientInfos).isNotNull();
    assertThat(clients.clientInfos).hasSize(3);
    for (int i = 0; i < 3; i++) {
      assertThat(clients.clientInfos.get(i).id).isEqualTo(expectedClients.get(i).id);
      assertThat(clients.clientInfos.get(i).age).isEqualTo(expectedClients.get(i).age);
    }

    assertThat(view.rowList).hasSize(expectedClients.size());
    for (int i = 0; i < expectedClients.size(); i++) {
      assertThat(view.rowList.get(i).age).isEqualTo(expectedClients.get(i).age);
    }
  }

  @Test
  public void testGetFilteredClientsInfoSortedByAgeDown() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    accountTestDao.get().clearAccounts();

    List<CharmDot> ch = genCharms();
    List<ClientDot> clientDots = genClients();
    List<ClientRecord> expectedClients = getClientRecordFromDot(clientDots);

    TestView view = new TestView();

    FilterSortParams filterSortParams = new FilterSortParams();
    filterSortParams.filterStr = "";
    filterSortParams.sortBy = "age";
    filterSortParams.sortOrder = "down";
    ClientsListParams clientsListParams = new ClientsListParams();
    clientsListParams.pageID = 1;
    clientsListParams.filterSortParams = filterSortParams;
    ClientsListReportParams clientsListReportParams = new ClientsListReportParams("Пушкин", view, filterSortParams);

    //
    //
    ClientToReturn clients = clientRegister.get().getClientsRecordList(clientsListParams);
    clientRegister.get().genClientListReport(clientsListReportParams);
    //
    //

    expectedClients.sort(new Comparator<ClientRecord>() {
      @Override
      public int compare(ClientRecord o1, ClientRecord o2) {
        return Integer.compare(o2.age, o1.age);
      }
    });

    assertThat(clients).isNotNull();
    assertThat(clients.pageCount).isEqualTo(10);
    assertThat(clients.clientInfos).isNotNull();
    assertThat(clients.clientInfos).hasSize(3);
    for (int i = 0; i < 3; i++) {
      assertThat(clients.clientInfos.get(i).id).isEqualTo(expectedClients.get(i).id);
      assertThat(clients.clientInfos.get(i).age).isEqualTo(expectedClients.get(i).age);
    }

    assertThat(view.rowList).hasSize(expectedClients.size());
    for (int i = 0; i < expectedClients.size(); i++) {
      assertThat(view.rowList.get(i).age).isEqualTo(expectedClients.get(i).age);
    }
  }

  @Test
  public void testGetFilteredClientsInfoSortedByCashUp() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    accountTestDao.get().clearAccounts();

    List<CharmDot> ch = genCharms();
    List<ClientDot> clientDots = genClients();
    List<ClientRecord> expectedClients = getClientRecordFromDot(clientDots);

    for(int i = 0; i < 30; i++) {
      AccountDot accountDot = new AccountDot();
      accountDot.id = i + 1;
      accountDot.money = (float) RND.plusInt(100000);
      accountDot.number = RND.str(10);
      accountDot.registered_at = Timestamp.valueOf(LocalDateTime.now());
      accountDot.clientID = RND.plusInt(30) + 1;
      accountTestDao.get().insertAccount(accountDot);

      expectedClients.get(accountDot.clientID - 1).totalCash += accountDot.money;
    }

    TestView view = new TestView();

    FilterSortParams filterSortParams = new FilterSortParams();
    filterSortParams.filterStr = "";
    filterSortParams.sortBy = "totalCash";
    filterSortParams.sortOrder = "up";
    ClientsListParams clientsListParams = new ClientsListParams();
    clientsListParams.pageID = 1;
    clientsListParams.filterSortParams = filterSortParams;
    ClientsListReportParams clientsListReportParams = new ClientsListReportParams("Пушкин", view, filterSortParams);

    //
    //
    ClientToReturn clients = clientRegister.get().getClientsRecordList(clientsListParams);
    clientRegister.get().genClientListReport(clientsListReportParams);
    //
    //

    expectedClients.sort(new Comparator<ClientRecord>() {
      @Override
      public int compare(ClientRecord o1, ClientRecord o2) {
        return Float.compare(o1.totalCash, o2.totalCash);
      }
    });

    assertThat(clients).isNotNull();
    assertThat(clients.pageCount).isEqualTo(10);
    assertThat(clients.clientInfos).isNotNull();
    assertThat(clients.clientInfos).hasSize(3);
    for (int i = 0; i < 3; i++) {
      assertThat(clients.clientInfos.get(i).id).isEqualTo(expectedClients.get(i).id);
      assertThat(clients.clientInfos.get(i).totalCash).isEqualTo(expectedClients.get(i).totalCash);
    }

    assertThat(view.rowList).hasSize(30);
    for (int i = 0; i < 30; i++) {
      assertThat(view.rowList.get(i).totalCash).isEqualTo(expectedClients.get(i).totalCash);
    }
  }

  @Test
  public void testGetFilteredClientsInfoSortedByCashDown() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    accountTestDao.get().clearAccounts();

    List<CharmDot> ch = genCharms();
    List<ClientDot> clientDots = genClients();
    List<ClientRecord> expectedClients = getClientRecordFromDot(clientDots);

    for(int i = 0; i < 30; i++) {
      AccountDot accountDot = new AccountDot();
      accountDot.id = i + 1;
      accountDot.money = (float) RND.plusInt(100000);
      accountDot.number = RND.str(10);
      accountDot.registered_at = Timestamp.valueOf(LocalDateTime.now());
      accountDot.clientID = RND.plusInt(30) + 1;
      accountTestDao.get().insertAccount(accountDot);

      expectedClients.get(accountDot.clientID - 1).totalCash += accountDot.money;
    }

    TestView view = new TestView();

    FilterSortParams filterSortParams = new FilterSortParams();
    filterSortParams.filterStr = "";
    filterSortParams.sortBy = "totalCash";
    filterSortParams.sortOrder = "down";
    ClientsListParams clientsListParams = new ClientsListParams();
    clientsListParams.pageID = 1;
    clientsListParams.filterSortParams = filterSortParams;
    ClientsListReportParams clientsListReportParams = new ClientsListReportParams("Пушкин", view, filterSortParams);

    //
    //
    ClientToReturn clients = clientRegister.get().getClientsRecordList(clientsListParams);
    clientRegister.get().genClientListReport(clientsListReportParams);
    //
    //

    expectedClients.sort(new Comparator<ClientRecord>() {
      @Override
      public int compare(ClientRecord o1, ClientRecord o2) {
        return Float.compare(o2.totalCash, o1.totalCash);
      }
    });

    assertThat(clients).isNotNull();
    assertThat(clients.pageCount).isEqualTo(10);
    assertThat(clients.clientInfos).isNotNull();
    assertThat(clients.clientInfos).hasSize(3);
    for (int i = 0; i < 3; i++) {
      assertThat(clients.clientInfos.get(i).id).isEqualTo(expectedClients.get(i).id);
      assertThat(clients.clientInfos.get(i).totalCash).isEqualTo(expectedClients.get(i).totalCash);
    }

    assertThat(view.rowList).hasSize(30);
    for (int i = 0; i < 30; i++) {
      assertThat(view.rowList.get(i).totalCash).isEqualTo(expectedClients.get(i).totalCash);
    }
  }

  @Test
  public void testGetCharms() throws Exception {
    charmTestDao.get().clearCharms();

    List<CharmDot> charmDots = genCharms();

    //
    //
    List<Charm> charms = clientRegister.get().getCharms();
    //
    //

    assertThat(charms).isNotNull();
    assertThat(charms).hasSize(4);

    charms.sort(new Comparator<Charm>() {
      @Override
      public int compare(Charm o1, Charm o2) {
        return o1.name.compareTo(o2.name);
      }
    });

    charmDots.sort(new Comparator<CharmDot>() {
      @Override
      public int compare(CharmDot o1, CharmDot o2) {
        return o1.name.compareTo(o2.name);
      }
    });

    for (int i = 0; i < 4; i++) {
      assertThat(charms.get(i).id).isEqualTo(charmDots.get(i).id);
      assertThat(charms.get(i).name).isEqualTo(charmDots.get(i).name);
      assertThat(charms.get(i).description).isEqualTo(charmDots.get(i).description);
    }

  }

  @Test
  public void testGetReportParams() {
    ReportParamsToSave reportParamsToSave = new ReportParamsToSave();
    reportParamsToSave.report_id = 1;
    reportParamsToSave.report_type = "PDF";
    reportParamsToSave.username = "Sanzhar";
    reportParamsToSave.filterStr = "";
    reportParamsToSave.sortBy = "";
    reportParamsToSave.sortOrder = "";

    //
    //
    clientRegister.get().saveReportParams(reportParamsToSave);
    ReportParamsToSave reportParamsToSave1 = clientRegister.get().popReportParams(1);
    //
    //

    assertThat(reportParamsToSave1.report_id).isEqualTo(1);
    assertThat(reportParamsToSave1.report_type).isNotNull();
    assertThat(reportParamsToSave1.username).isNotNull();
    assertThat(reportParamsToSave1.filterStr).isNotNull();
    assertThat(reportParamsToSave1.sortBy).isNotNull();
    assertThat(reportParamsToSave1.sortOrder).isNotNull();
  }

  private static class TestView implements ClientsListReportView {

    public String title;
    public String userName;

    @Override
    public void start(String title) {
      this.title = title;
    }

    List<ClientListRow> rowList = new ArrayList<ClientListRow>();

    @Override
    public void append(ClientListRow clientListRow) {
      rowList.add(clientListRow);
    }

    @Override
    public void finish(String userName) {
      this.userName = userName;
    }
  }

  @Test
  public void genClientListReport() throws Exception {
    clientTestDao.get().clearClients();
    charmTestDao.get().clearCharms();
    accountTestDao.get().clearAccounts();

    standDb.get().charmStorage.values().stream()
      .forEach(charmTestDao.get()::insertCharm);
    standDb.get().clientStorage.values().stream()
      .forEach(clientTestDao.get()::insertClient);
    standDb.get().accountStorage.values().stream()
      .forEach(accountTestDao.get()::insertAccount);

    TestView view = new TestView();

    FilterSortParams filterSortParams = new FilterSortParams();
    filterSortParams.filterStr = "";
    filterSortParams.sortBy = "";
    filterSortParams.sortOrder = "";

    ClientsListReportParams clientsListReportParams = new ClientsListReportParams("Pushkin", view, filterSortParams);
    //
    //
    clientRegister.get().genClientListReport(clientsListReportParams);
    //
    //

    view.rowList.sort(new Comparator<ClientListRow>() {
      @Override
      public int compare(ClientListRow o1, ClientListRow o2) {
        return (o1.fio.compareTo(o2.fio));
      }
    });
    assertThat(view.rowList).hasSize(4);
    assertThat(view.rowList.get(3).fio).isEqualTo("Толстой Лев Николаевич");
    assertThat(view.rowList.get(1).fio).isEqualTo("Лермонтов Михаил Юрьевич");
    assertThat(view.rowList.get(0).fio).isEqualTo("Бурумбай Санжар Ришадулы");
    assertThat(view.rowList.get(2).fio).isEqualTo("Пушкин Александр Сергеевич");
  }


  private List<CharmDot> genCharms() {
    List<CharmDot> charms = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      CharmDot charmDot = new CharmDot();
      charmDot.id = i + 1;
      charmDot.name = RND.str(10);
      charmDot.description = RND.str(20);
      charmDot.energy = (float) RND.plusInt(1000) + 1;
      charmTestDao.get().insertCharm(charmDot);
      charms.add(charmDot);
    }

    return charms;
  }

  private List<ClientDot> genClients() {
    List<ClientDot> clients = new ArrayList<>();

    for (int i = 1; i <= 30; i++) {
      ClientDot clientDot = new ClientDot();
      clientDot.id = i;
      clientDot.name = RND.str(10);
      clientDot.surname = RND.str(10);
      clientDot.patronymic = RND.str(10);
      clientDot.birth_date = RND.dateYears(1990, 2015);
      clientDot.gender = RND.str(4);
      clientDot.charm_id = RND.plusInt(4) + 1;
      clientTestDao.get().insertClient(clientDot);
      clients.add(clientDot);
    }

    return clients;
  }

  private List<ClientRecord> getClientRecordFromDot(List<ClientDot> clientDots) {
    List<ClientRecord> expectedClients = new ArrayList<>();
    for(ClientDot clientDot : clientDots) {
      ClientRecord expectedClientRecord = new ClientRecord();
      expectedClientRecord.fio = clientDot.surname + " " + clientDot.name + " " + clientDot.patronymic;
      expectedClientRecord.id = clientDot.id;
      expectedClientRecord.age = clientDot.countAge();

      expectedClients.add(expectedClientRecord);
    }

    return expectedClients;
  }
}
