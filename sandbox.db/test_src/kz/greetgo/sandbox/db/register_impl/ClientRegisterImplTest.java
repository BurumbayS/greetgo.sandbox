package kz.greetgo.sandbox.db.register_impl;

import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.sandbox.controller.errors.NotFound;
import kz.greetgo.sandbox.controller.model.*;
import kz.greetgo.sandbox.controller.register.ClientRegister;
import kz.greetgo.sandbox.db.test.dao.CharmTestDao;
import kz.greetgo.sandbox.db.test.dao.ClientTestDao;
import kz.greetgo.sandbox.db.test.util.ParentTestNg;
import kz.greetgo.sandbox.db.util.PageUtils;
import kz.greetgo.util.RND;
import org.apache.ibatis.annotations.Insert;
import org.testng.annotations.Test;

import java.sql.Date;
import java.time.LocalDate;
import java.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

/**
 * Набор автоматизированных тестов для тестирования методов класса {@link ClientRegisterImpl}
 */
public class ClientRegisterImplTest extends ParentTestNg {

  public BeanGetter<ClientRegister> clientRegister;
  public BeanGetter<ClientTestDao> clientTestDao;
  public BeanGetter<CharmTestDao> charmTestDao;
  public BeanGetter<IdGenerator> idGen;

  @Test
  public void getTotalSize_noFilter() {

    List<ClientDetails> clients = clearDbAndInsertTestData(100);

    assertThat(clients).isNotNull();

    //
    //
    long result = clientRegister.get().getTotalSize("", "");
    //
    //

    assertThat(result).isEqualTo(clients.size());
  }

  @Test
  public void getTotalSize_filteredBySurname() {

    List<ClientDetails> clients = clearDbAndInsertTestData(100);

    String filterInput = clients.get(RND.plusInt(clients.size())).surname.toLowerCase().substring(7);
    int count = 0;
    for (ClientDetails client : clients) {
      if (client.surname.toLowerCase().contains(filterInput)) count++;
    }

    //
    //
    long result = clientRegister.get().getTotalSize("surname", filterInput);
    //
    //

    assertThat(result).isEqualTo(count);
  }

  @Test
  public void getTotalSize_filteredByName() {

    List<ClientDetails> clients = clearDbAndInsertTestData(100);

    String filterInput = clients.get(RND.plusInt(clients.size())).name.toLowerCase().substring(7);
    int count = 0;
    for (ClientDetails client : clients) {
      if (client.name.toLowerCase().contains(filterInput)) count++;
    }

    //
    //
    long result = clientRegister.get().getTotalSize("name", filterInput);
    //
    //

    assertThat(result).isEqualTo(count);
  }

  @Test
  public void getTotalSize_filteredByPatronymic() {

    List<ClientDetails> clients = clearDbAndInsertTestData(100);

    String filterInput = clients.get(RND.plusInt(clients.size())).patronymic.toLowerCase().substring(7);
    int count = 0;
    for (ClientDetails client : clients) {
      if (client.patronymic.toLowerCase().contains(filterInput)) count++;
    }

    //
    //
    long result = clientRegister.get().getTotalSize("patronymic", filterInput);
    //
    //

    assertThat(result).isEqualTo(count);
  }

  @Test
  public void getClientsList_default() {

    List<ClientDetails> clients = clearDbAndInsertTestData(200);

//    int pageSize = RND.plusInt(clients.size());
//    int page = pageSize > 0 ? RND.plusInt((int) Math.ceil(clients.size() / pageSize)) : 0;
    int pageSize = 25;
    int page = 1;

    List<ClientInfo> expectingClientList = new ArrayList<>();
    clients.forEach(clientDetails -> expectingClientList.add(toClientInfo(clientDetails)));

    expectingClientList.sort(Comparator.comparing(o -> o.surname.toLowerCase()));

    PageUtils.cutPage(expectingClientList, page * pageSize, pageSize);

    //
    //
    List<ClientInfo> result = clientRegister.get().getClientsList("", "", "",
      false, page, pageSize);
    //
    //

    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(expectingClientList.size());
    for (int i = 0; i < expectingClientList.size(); i++) {
      assertThatAreEqual(result.get(i), expectingClientList.get(i));
    }
  }

  @Test
  public void getClientsList_orderedByAge() {

    List<ClientDetails> clients = clearDbAndInsertTestData(200);

    int pageSize = RND.plusInt(clients.size());
    int page = pageSize > 0 ? RND.plusInt((int) Math.ceil(clients.size() / pageSize)) : 0;

    List<ClientInfo> expectingClientList = new ArrayList<>();
    clients.forEach(clientDetails -> expectingClientList.add(toClientInfo(clientDetails)));

//    expectingClientList.sort(Comparator.comparingInt(o -> o.age));
    Collections.sort(expectingClientList, new Comparator() {

      public int compare(Object o1, Object o2) {

        Integer tb1 = ((ClientInfo) o1).age;
        Integer tb2 = ((ClientInfo) o2).age;
        int sComp = tb1.compareTo(tb2);

        if (sComp != 0) {
          return sComp;
        } else {
          String sn1 = ((ClientInfo) o1).surname.toLowerCase();
          String sn2 = ((ClientInfo) o2).surname.toLowerCase();
          return sn1.compareTo(sn2);
        }
      }});

    PageUtils.cutPage(expectingClientList, page * pageSize, pageSize);

    //
    //
    List<ClientInfo> result = clientRegister.get().getClientsList("", "", "age",
      false, page, pageSize);
    //
    //

    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(expectingClientList.size());
    for (int i = 0; i < expectingClientList.size(); i++) {
      assertThatAreEqual(result.get(i), expectingClientList.get(i));
    }
  }

  private void assertThatAreEqual(ClientInfo ci1, ClientInfo ci2) {
    assertThat(ci1.id).isEqualTo(ci2.id);
    assertThat(ci1.surname).isEqualTo(ci2.surname);
    assertThat(ci1.name).isEqualTo(ci2.name);
    assertThat(ci1.patronymic).isEqualTo(ci2.patronymic);
    assertThat(ci1.charm.id).isEqualTo(ci2.charm.id);
    assertThat(ci1.age).isEqualTo(ci2.age);
    assertThat(Math.abs(ci1.totalBalance - ci2.totalBalance)).isLessThan(0.001);
    assertThat(Math.abs(ci1.minBalance - ci2.minBalance)).isLessThan(0.001);
    assertThat(Math.abs(ci1.maxBalance - ci2.maxBalance)).isLessThan(0.001);
  }

  @Test
  public void getClientsList_orderedByTotalBalance() {

    List<ClientDetails> clients = clearDbAndInsertTestData(200);

    int pageSize = RND.plusInt(clients.size());
    int page = pageSize > 0 ? RND.plusInt((int) Math.ceil(clients.size() / pageSize)) : 0;

//    int pageSize = 25;
//    int page = 0;

    List<ClientInfo> expectingClientList = new ArrayList<>();
    clients.forEach(clientDetails -> expectingClientList.add(toClientInfo(clientDetails)));

//    expectingClientList.sort(Comparator.comparingDouble(o -> o.totalBalance));
    Collections.sort(expectingClientList, new Comparator() {

      public int compare(Object o1, Object o2) {

        Double tb1 = ((ClientInfo) o1).totalBalance;
        Double tb2 = ((ClientInfo) o2).totalBalance;
        int sComp = tb1.compareTo(tb2);

        if (sComp != 0) {
          return sComp;
        } else {
          String sn1 = ((ClientInfo) o1).surname.toLowerCase();
          String sn2 = ((ClientInfo) o2).surname.toLowerCase();
          return sn1.compareTo(sn2);
        }
      }});

    PageUtils.cutPage(expectingClientList, page * pageSize, pageSize);

    //
    //
    List<ClientInfo> result = clientRegister.get().getClientsList("", "", "totalBalance",
      false, page, pageSize);
    //
    //

    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(expectingClientList.size());
    for (int i = 0; i < expectingClientList.size(); i++) {
      assertThatAreEqual(result.get(i), expectingClientList.get(i));
    }
  }

  @Test
  public void getClientsList_descOrderedByTotalBalance() {

    List<ClientDetails> clients = clearDbAndInsertTestData(200);

    int pageSize = RND.plusInt(clients.size());
    int page = pageSize > 0 ? RND.plusInt((int) Math.ceil(clients.size() / pageSize)) : 0;
//    int pageSize = 25;
//    int page = 1;

    List<ClientInfo> expectingClientList = new ArrayList<>();
    clients.forEach(clientDetails -> expectingClientList.add(toClientInfo(clientDetails)));

//    expectingClientList.sort(Comparator.comparingDouble(o -> o.totalBalance));
    Collections.sort(expectingClientList, new Comparator() {

      public int compare(Object o1, Object o2) {

        Double tb1 = ((ClientInfo) o1).totalBalance;
        Double tb2 = ((ClientInfo) o2).totalBalance;
        int sComp = tb1.compareTo(tb2);

        if (sComp != 0) {
          return sComp;
        } else {
          String sn1 = ((ClientInfo) o1).surname.toLowerCase();
          String sn2 = ((ClientInfo) o2).surname.toLowerCase();
          return sn1.compareTo(sn2);
        }
      }});
    Collections.reverse(expectingClientList);
//    Collections.sort(expectingClientList, new Comparator() {
//
//      public int compare(Object o1, Object o2) {
//
//        Double tb1 = ((ClientInfo) o1).totalBalance;
//        Double tb2 = ((ClientInfo) o2).totalBalance;
//        int sComp = tb2.compareTo(tb1);
//
//        if (sComp != 0) {
//          return sComp;
//        } else {
//          String sn1 = ((ClientInfo) o1).surname.toLowerCase();
//          String sn2 = ((ClientInfo) o2).surname.toLowerCase();
//          return sn2.compareTo(sn1);
//        }
//      }});

    PageUtils.cutPage(expectingClientList, page * pageSize, pageSize);

    //
    //
    List<ClientInfo> result = clientRegister.get().getClientsList("", "", "totalBalance",
      true, page, pageSize);
    //
    //

    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(expectingClientList.size());
    for (int i = 0; i < expectingClientList.size(); i++) {
      assertThatAreEqual(result.get(i), expectingClientList.get(i));
    }
  }

  @Test
  public void getClientsList_orderedByMinBalance() {

    List<ClientDetails> clients = clearDbAndInsertTestData(200);

    int pageSize = RND.plusInt(clients.size());
    int page = pageSize > 0 ? RND.plusInt((int) Math.ceil(clients.size() / pageSize)) : 0;

//    int pageSize = 25;
//    int page = 1;

    List<ClientInfo> expectingClientList = new ArrayList<>();
    clients.forEach(clientDetails -> expectingClientList.add(toClientInfo(clientDetails)));

//    expectingClientList.sort(Comparator.comparingDouble(o -> o.minBalance));
    Collections.sort(expectingClientList, new Comparator() {

      public int compare(Object o1, Object o2) {

        Double tb1 = ((ClientInfo) o1).minBalance;
        Double tb2 = ((ClientInfo) o2).minBalance;
        int sComp = tb1.compareTo(tb2);

        if (sComp != 0) {
          return sComp;
        } else {
          String sn1 = ((ClientInfo) o1).surname.toLowerCase();
          String sn2 = ((ClientInfo) o2).surname.toLowerCase();
          return sn1.compareTo(sn2);
        }
      }});

    PageUtils.cutPage(expectingClientList, page * pageSize, pageSize);

    //
    //
    List<ClientInfo> result = clientRegister.get().getClientsList("", "", "minBalance",
      false, page, pageSize);
    //
    //

    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(expectingClientList.size());
    for (int i = 0; i < expectingClientList.size(); i++) {
      assertThatAreEqual(result.get(i), expectingClientList.get(i));
    }
  }

  @Test
  public void getClientsList_orderedByMaxBalance() {

    List<ClientDetails> clients = clearDbAndInsertTestData(200);

    int pageSize = RND.plusInt(clients.size());
    int page = pageSize > 0 ? RND.plusInt((int) Math.ceil(clients.size() / pageSize)) : 0;

//    int pageSize = 25;
//    int page = 1;

    List<ClientInfo> expectingClientList = new ArrayList<>();
    clients.forEach(clientDetails -> expectingClientList.add(toClientInfo(clientDetails)));

//    expectingClientList.sort(Comparator.comparingDouble(o -> o.maxBalance));
    Collections.sort(expectingClientList, new Comparator() {

      public int compare(Object o1, Object o2) {

        Double tb1 = ((ClientInfo) o1).maxBalance;
        Double tb2 = ((ClientInfo) o2).maxBalance;
        int sComp = tb1.compareTo(tb2);

        if (sComp != 0) {
          return sComp;
        } else {
          String sn1 = ((ClientInfo) o1).surname.toLowerCase();
          String sn2 = ((ClientInfo) o2).surname.toLowerCase();
          return sn1.compareTo(sn2);
        }
      }});

    PageUtils.cutPage(expectingClientList, page * pageSize, pageSize);

    //
    //
    List<ClientInfo> result = clientRegister.get().getClientsList("", "", "maxBalance",
      false, page, pageSize);
    //
    //

    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(expectingClientList.size());
    for (int i = 0; i < expectingClientList.size(); i++) {
      assertThatAreEqual(result.get(i), expectingClientList.get(i));
    }
  }

  @Test
  public void getClientsList_filteredBySurname() {

    List<ClientDetails> clients = clearDbAndInsertTestData(200);

    List<ClientInfo> clientInfos = new ArrayList<>();
    clients.forEach(clientDetails -> clientInfos.add(toClientInfo(clientDetails)));

    String filterInput = clients.get(RND.plusInt(clients.size())).surname.toLowerCase().substring(7);

    List<ClientInfo> expectingClientList = filterClientList(clientInfos, "surname", filterInput);

    expectingClientList.sort(Comparator.comparing(o -> o.surname));

    int pageSize = RND.plusInt(expectingClientList.size());
    int page = 0;
    if (pageSize > 0) {
      page = RND.plusInt((int) Math.ceil(expectingClientList.size() / pageSize));
    } else {
      pageSize = expectingClientList.size();
    }

    PageUtils.cutPage(expectingClientList, page * pageSize, pageSize);

    //
    //
    List<ClientInfo> result = clientRegister.get().getClientsList("surname", filterInput, "",
      false, page, pageSize);
    //
    //

    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(expectingClientList.size());
    for (int i = 0; i < expectingClientList.size(); i++) {
      assertThatAreEqual(result.get(i), expectingClientList.get(i));
    }
  }

  @Test
  public void getClientsList_filteredByName() {

    List<ClientDetails> clients = clearDbAndInsertTestData(200);

    List<ClientInfo> clientInfos = new ArrayList<>();
    clients.forEach(clientDetails -> clientInfos.add(toClientInfo(clientDetails)));

    String filterInput = clients.get(RND.plusInt(clients.size())).name.toLowerCase().substring(7);

    List<ClientInfo> expectingClientList = filterClientList(clientInfos, "name", filterInput);

    expectingClientList.sort(Comparator.comparing(o -> o.surname));

    int pageSize = RND.plusInt(expectingClientList.size());
    int page = 0;
    if (pageSize > 0) {
      page = RND.plusInt((int) Math.ceil(expectingClientList.size() / pageSize));
    } else {
      pageSize = expectingClientList.size();
    }

    PageUtils.cutPage(expectingClientList, page * pageSize, pageSize);

    //
    //
    List<ClientInfo> result = clientRegister.get().getClientsList("name", filterInput, "",
      false, page, pageSize);
    //
    //

    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(expectingClientList.size());
    for (int i = 0; i < expectingClientList.size(); i++) {
      assertThatAreEqual(result.get(i), expectingClientList.get(i));
    }
  }

  @Test
  public void getClientsList_filteredByPatronymic() {

    List<ClientDetails> clients = clearDbAndInsertTestData(200);

    List<ClientInfo> clientInfos = new ArrayList<>();
    clients.forEach(clientDetails -> clientInfos.add(toClientInfo(clientDetails)));

    String filterInput = clients.get(RND.plusInt(clients.size())).patronymic.toLowerCase().substring(7);

    List<ClientInfo> expectingClientList = filterClientList(clientInfos, "patronymic", filterInput);

    expectingClientList.sort(Comparator.comparing(o -> o.surname));

    int pageSize = RND.plusInt(expectingClientList.size());
    int page = 0;
    if (pageSize > 0) {
      page = RND.plusInt((int) Math.ceil(expectingClientList.size() / pageSize));
    } else {
      pageSize = expectingClientList.size();
    }

    PageUtils.cutPage(expectingClientList, page * pageSize, pageSize);

    //
    //
    List<ClientInfo> result = clientRegister.get().getClientsList("patronymic", filterInput, "",
      false, page, pageSize);
    //
    //

    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(expectingClientList.size());
    for (int i = 0; i < expectingClientList.size(); i++) {
      assertThatAreEqual(result.get(i), expectingClientList.get(i));
    }
  }

  @Test
  public void getClientsList_filteredByPatronymicAndOrderedByMinBalance() {

    List<ClientDetails> clients = clearDbAndInsertTestData(200);

    List<ClientInfo> clientInfos = new ArrayList<>();
    clients.forEach(clientDetails -> clientInfos.add(toClientInfo(clientDetails)));

    String filterInput = clients.get(RND.plusInt(clients.size())).patronymic.toLowerCase().substring(7);

    List<ClientInfo> expectingClientList = filterClientList(clientInfos, "patronymic", filterInput);

    expectingClientList.sort(Comparator.comparingDouble(o -> o.minBalance));

    int pageSize = RND.plusInt(expectingClientList.size());
    int page = 0;
    if (pageSize > 0) {
      page = RND.plusInt((int) Math.ceil(expectingClientList.size() / pageSize));
    } else {
      pageSize = expectingClientList.size();
    }

    PageUtils.cutPage(expectingClientList, page * pageSize, pageSize);

    //
    //
    List<ClientInfo> result = clientRegister.get().getClientsList("patronymic", filterInput, "minBalance",
      false, page, pageSize);
    //
    //

    assertThat(result).isNotNull();
    assertThat(result.size()).isEqualTo(expectingClientList.size());
    for (int i = 0; i < expectingClientList.size(); i++) {
      assertThatAreEqual(result.get(i), expectingClientList.get(i));
    }
  }

  @Test
  public void getClientDetails_ok() {

    List<ClientDetails> clients = clearDbAndInsertTestData(10);

    ClientDetails expectingClient = clients.get(RND.plusInt(clients.size()));

    //
    //
    ClientDetails result = clientRegister.get().getClientDetails(expectingClient.id);
    //
    //

    assertThat(result).isNotNull();
    assertThat(result.id).isEqualTo(expectingClient.id);
    assertThat(result.surname).isEqualTo(expectingClient.surname);
    assertThat(result.name).isEqualTo(expectingClient.name);
    assertThat(result.patronymic).isEqualTo(expectingClient.patronymic);
    assertThat(result.gender).isEqualTo(expectingClient.gender);
    assertThat(result.dateOfBirth).isEqualTo(expectingClient.dateOfBirth);
    assertThat(result.charm.id).isEqualTo(expectingClient.charm.id);
    assertThat(result.addressF.street).isEqualTo(expectingClient.addressF.street);
    assertThat(result.addressF.house).isEqualTo(expectingClient.addressF.house);
    assertThat(result.addressF.flat).isEqualTo(expectingClient.addressF.flat);
    assertThat(result.addressR.street).isEqualTo(expectingClient.addressR.street);
    assertThat(result.addressR.house).isEqualTo(expectingClient.addressR.house);
    assertThat(result.addressR.flat).isEqualTo(expectingClient.addressR.flat);
    result.phoneNumbers.sort(Comparator.comparing(phoneNumber -> phoneNumber.number.toLowerCase()));
    expectingClient.phoneNumbers.sort(Comparator.comparing(phoneNumber -> phoneNumber.number.toLowerCase()));
    for (int i = 0; i < result.phoneNumbers.size(); i++) {
      assertThat(result.phoneNumbers.get(i).number).isEqualTo(expectingClient.phoneNumbers.get(i).number);
      assertThat(result.phoneNumbers.get(i).phoneType).isEqualTo(expectingClient.phoneNumbers.get(i).phoneType);
    }
    assertThat(result.charm.id).isEqualTo(expectingClient.charm.id);
  }

  @Test(expectedExceptions = NotFound.class)
  public void getClientDetails_NotFound() throws Exception {

    List<ClientDetails> clients = clearDbAndInsertTestData(10);

    //
    //
    ClientDetails result = clientRegister.get().getClientDetails(idGen.get().newId());
    //
    //
  }

  @Test
  public void addOrUpdateClient_add() {
    clientTestDao.get().removeAllData();


    ClientDetails clientDetails = createRndClient();
    ClientInfo expectingClientInfo = toClientInfo(clientDetails);
    ClientRecords clientRecords = toClientRecords(clientDetails);

    charmTestDao.get().insertCharm(clientRecords.charm.id, clientRecords.charm.name,
      clientRecords.charm.description, clientRecords.charm.energy);

    //
    //
    ClientInfo result = clientRegister.get().addOrUpdateClient(clientRecords);
    //
    //

    assertThat(result).isNotNull();
    assertThatAreEqual(result, expectingClientInfo);
  }

  @Test
  public void addOrUpdateClient_update() {
    clientTestDao.get().removeAllData();

    ClientDetails client = createRndClient();

    assertThat(client).isNotNull();

    charmTestDao.get().insertCharm(client.charm.id, client.charm.name,
      client.charm.description, client.charm.energy);

    clientTestDao.get().insertClient(client.id, client.surname, client.name,
      client.patronymic, client.gender, Date.valueOf(client.dateOfBirth), client.charm.id);

    ClientDetails clientDetails = createRndClient();
    clientDetails.id = client.id;

    ClientInfo expectingClientInfo = toClientInfo(clientDetails);
    ClientRecords clientRecords = toClientRecords(clientDetails);

    charmTestDao.get().insertCharm(clientRecords.charm.id, clientRecords.charm.name,
      clientRecords.charm.description, clientRecords.charm.energy);

    //
    //
    ClientInfo result = clientRegister.get().addOrUpdateClient(clientRecords);
    //
    //

    assertThat(result).isNotNull();
    assertThatAreEqual(result, expectingClientInfo);
  }

  @Test
  public void removeClient_ok() {
    clientTestDao.get().removeAllData();

    ClientDetails client = createRndClient();

    charmTestDao.get().insertCharm(client.charm.id, client.charm.name,
      client.charm.description, client.charm.energy);

    clientTestDao.get().insertClient(client.id, client.surname, client.name,
      client.patronymic, client.gender, Date.valueOf(client.dateOfBirth), client.charm.id);

    assertThat(client).isNotNull();

    //
    //
    clientRegister.get().removeClient(client.id);
    //
    //

    assertThat(clientTestDao.get().getClientById(client.id)).isNull();
  }

  @Test
  public void removeClient_NotFound() throws Exception {
    clientTestDao.get().removeAllData();

    ClientDetails client = createRndClient();

    charmTestDao.get().insertCharm(client.charm.id, client.charm.name,
      client.charm.description, client.charm.energy);

    clientTestDao.get().insertClient(client.id, client.surname, client.name,
      client.patronymic, client.gender, Date.valueOf(client.dateOfBirth), client.charm.id);

    assertThat(client).isNotNull();

    //
    //
    clientRegister.get().removeClient(idGen.get().newId());
    //
    //

    assertThat(clientTestDao.get().getClientById(client.id)).isNotNull();
  }

  private List<ClientDetails> clearDbAndInsertTestData(int size) {
    clientTestDao.get().removeAllData();
    List<ClientDetails> clients = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      ClientDetails client = createRndClient();
      clientTestDao.get().insertCharm(client.charm.id, client.charm.name, client.charm.description, client.charm.energy);
      clientTestDao.get().insertClient(client.id, client.surname, client.name,
        client.patronymic, client.gender, Date.valueOf(client.dateOfBirth), client.charm.id);
      clientTestDao.get().insertAddress(client.id, client.addressF.type, client.addressF.street, client.addressF.house, client.addressF.flat);
      clientTestDao.get().insertAddress(client.id, client.addressR.type, client.addressR.street, client.addressR.house, client.addressR.flat);
      for (PhoneNumber phoneNumber : client.phoneNumbers) {
        clientTestDao.get().insertPhoneNumber(client.id, phoneNumber.number, phoneNumber.phoneType);
      }
//      for (int j = 0; j < client.phoneNumbers.size(); j++) {
//        clientTestDao.get().insertPhoneNumber(client.id, client.phoneNumbers.get(j).number, client.phoneNumbers.get(j).phoneType);
//      }
      // TODO: 2/16/18 type of registeredAt timestamp should be OffsetDateTime
      double total = 0.0;
      double min = 1000.0;
      double max = 0.0;
      for (int j = 0; j < RND.plusInt(4); j++) {
        double money = RND.plusDouble(1000, 2);
        total += money;
        if (money < min) min = money;
        if (money > max) max = money;
        clientTestDao.get().insertClientAccount(idGen.get().newId(), client.id, money,
          RND.str(10), null);
      }
      client.totalBalance = total;
      client.minBalance = min < 1000.0 ? min : 0.0;
      client.maxBalance = max;
      clients.add(client);
    }
    return clients;
  }

  private ClientDetails createRndClient() {
    ClientDetails client = new ClientDetails();
    client.id = idGen.get().newId();
    client.surname = (10000 + RND.plusInt(99999)) + RND.str(5);
    client.name = RND.str(10);
    client.patronymic = RND.str(10);
    client.charm = new Charm();
    client.charm.id = idGen.get().newId();
    client.charm.name = RND.str(10);
    client.charm.description = RND.str(10);
    client.charm.energy = RND.plusDouble(100, 2);
    client.gender = RND.someEnum(Gender.values());
    client.dateOfBirth = LocalDate.now().toString();
    client.addressF = new Address();
    client.addressF.type = AddressType.FACT;
    client.addressF.street = RND.str(10);
    client.addressF.house = RND.str(5);
    client.addressF.flat = RND.str(5);
    client.addressR = new Address();
    client.addressR.type = AddressType.REG;
    client.addressR.street = RND.str(10);
    client.addressR.house = RND.str(5);
    client.addressR.flat = RND.str(5);
    for (int i = 0; i < RND.plusInt(2) + 3; i++) {
      PhoneNumber phoneNumber = new PhoneNumber();
      phoneNumber.phoneType = RND.someEnum(PhoneType.values());
      phoneNumber.number = RND.str(10);
      client.phoneNumbers.add(phoneNumber);
    }
    return client;
  }

  private List<ClientInfo> filterClientList(List<ClientInfo> clientInfos, String filterBy, String filterInput) {
    List<ClientInfo> clientList = new ArrayList<>();
    for (ClientInfo clientInfo : clientInfos) {
      if ("surname".equals(filterBy) && clientInfo.surname.toLowerCase().contains(filterInput.toLowerCase()))
        clientList.add(clientInfo);
      else if ("name".equals(filterBy) && clientInfo.name.toLowerCase().contains(filterInput.toLowerCase()))
        clientList.add(clientInfo);
      else if ("patronymic".equals(filterBy) && clientInfo.patronymic.toLowerCase().contains(filterInput.toLowerCase()))
        clientList.add(clientInfo);
    }
    return clientList;
  }

  private ClientInfo toClientInfo(ClientDetails clientDetails) {
    ClientInfo clientInfo = new ClientInfo();
    clientInfo.id = clientDetails.id;
    clientInfo.surname = clientDetails.surname;
    clientInfo.name = clientDetails.name;
    clientInfo.patronymic = clientDetails.patronymic;
    clientInfo.charm = clientDetails.charm;
    clientInfo.age = clientDetails.dateOfBirth != null ? Period.between(LocalDate.parse(clientDetails.dateOfBirth),
      LocalDate.now()).getYears() : 0;
    clientInfo.totalBalance = clientDetails.totalBalance;
    clientInfo.minBalance = clientDetails.minBalance;
    clientInfo.maxBalance = clientDetails.maxBalance;
    return clientInfo;
  }

  private ClientRecords toClientRecords(ClientDetails clientDetails) {
    ClientRecords clientRecords = new ClientRecords();
    clientRecords.id = clientDetails.id;
    clientRecords.surname = clientDetails.surname;
    clientRecords.name = clientDetails.name;
    clientRecords.patronymic = clientDetails.patronymic;
    clientRecords.charm = clientDetails.charm;
    clientRecords.gender = clientDetails.gender;
    clientRecords.dateOfBirth = clientDetails.dateOfBirth;
    clientRecords.addressF = clientDetails.addressF;
    clientRecords.addressR = clientDetails.addressR;
    clientRecords.phoneNumbers = clientDetails.phoneNumbers;
    clientRecords.totalBalance = clientDetails.totalBalance;
    clientRecords.minBalance = clientDetails.minBalance;
    clientRecords.maxBalance = clientDetails.maxBalance;
    return clientRecords;
  }
}
