package kz.greetgo.sandbox.stand.stand_register_impls;


import kz.greetgo.depinject.core.Bean;
import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.sandbox.controller.model.ClientDetails;
import kz.greetgo.sandbox.controller.model.ClientListRequest;
import kz.greetgo.sandbox.controller.model.ClientRecord;
import kz.greetgo.sandbox.controller.model.ClientToSave;
import kz.greetgo.sandbox.controller.register.ClientRegister;
import kz.greetgo.sandbox.db.stand.beans.StandClientDb;
import kz.greetgo.sandbox.db.stand.beans.StandDb;
import kz.greetgo.sandbox.db.stand.model.ClientDot;
import kz.greetgo.util.ServerUtil;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@Bean
public class ClientRegisterStand implements ClientRegister {
  public BeanGetter<StandClientDb> al;

  @Override
  public long getSize(ClientListRequest clientListRequest) {
    if ("".equals(clientListRequest.filterByFio)) {
      return al.get().clientStorage.size();
    } else return 0;
  }


  @Override
  public List<ClientRecord> getList(ClientListRequest clientListRequest) {
    List<ClientDot> fullList = new ArrayList(al.get().clientStorage.values());
    List<ClientRecord> list = new ArrayList<>();

    System.out.println("List Info" + clientListRequest.count + " " + clientListRequest.sort);

    for (int i = clientListRequest.skipFirst; i < clientListRequest.skipFirst+clientListRequest.count; i++) {
      list.add(fullList.get(i).toClientRecord());
      if (clientListRequest.skipFirst + clientListRequest.count > fullList.size()) break;
    }

    ClientRecord nn = fullList.get(3).toClientRecord();

    List<ClientRecord> list2 = new ArrayList<>();
    //list2.add(nn);

    // FIXME: 1/5/18 list2 всегда пустой. фильтрация не работает
    if ("".equals(clientListRequest.filterByFio)) return list;
    else return list2;
  }

  @Override
  public ClientDetails getClient(String id) {
    if (id != null) return al.get().clientStorage.get(id).toClientDetails();
    else {
      ClientDetails det = new ClientDetails();
      // FIXME: 1/5/18 выборка по айди не работает
      det.charms = al.get().clientStorage.get("1").toClientDetails().charms;
      return det;
    }
  }


  // FIXME: 1/5/18 Почему объявлен глобавльный атрибут? И почему у глобального атрибута такое непонятное название?
  int i = 50;

  @Override
  public ClientRecord saveClient(ClientToSave clientToSave) {
    i++;
    ClientDot clientDot = new ClientDot();
    if (clientToSave.id == null) clientToSave.id = Integer.toString(i);
    else clientDot = al.get().clientStorage.get(clientToSave.id);


////////////////////////////////////////////////////////////////////////////////
    clientDot.id = clientToSave.id;
    clientDot.name = clientToSave.name;
    clientDot.surname = clientToSave.surname;
    clientDot.patronymic = clientToSave.patronymic;
    clientDot.gender = clientToSave.gender;
    clientDot.temper = clientToSave.charmId;
    clientDot.dateOfBirth = clientToSave.dateOfBirth;

    System.out.println("client phones mobile" + clientToSave.phones.mobile);

    al.get().clientStorage.put(clientToSave.id, clientDot);

    return al.get().clientStorage.get(clientToSave.id).toClientRecord();
  }

  @Override
  public void deleteClient(String id) {
    al.get().clientStorage.remove(id);
  }

  @Override
  public void download(ClientListRequest clientListRequest,
                       OutputStream outputStream,
                       String contentType,
                       String personId) throws Exception {

    if (contentType.contains("pdf")) {
      try (InputStream in = StandDb.class.getResourceAsStream("getClientListReport.pdf")) {
        ServerUtil.copyStreamsAndCloseIn(in, outputStream);
      }

      return;
    }

    if ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".equals(contentType)) {
      try (InputStream in = StandDb.class.getResourceAsStream("getClientListReport.xlsx")) {
        ServerUtil.copyStreamsAndCloseIn(in, outputStream);
      }

      return;
    }

    throw new RuntimeException("Unknown content type " + contentType);
  }
}
