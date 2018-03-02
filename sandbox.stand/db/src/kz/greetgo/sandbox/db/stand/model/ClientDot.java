package kz.greetgo.sandbox.db.stand.model;

import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.sandbox.controller.enums.GenderType;
import kz.greetgo.sandbox.controller.model.ClientDetail;
import kz.greetgo.sandbox.controller.model.ClientRecord;
import kz.greetgo.sandbox.controller.model.ClientToSave;
import kz.greetgo.sandbox.db.stand.beans.StandDb;
import kz.greetgo.sandbox.db.stand.tools.AgeCalculator;

import java.util.Date;

public class ClientDot {

  private BeanGetter<StandDb> db;

  public String id;
  public String surname;
  public String name;
  public String patronymic;
  public Date birthDate;
  public GenderType gender;
  public String charm;

  @SuppressWarnings("StringBufferReplaceableByString")
  public String getFIO() {
    return new StringBuilder(this.surname).append(this.name).append(this.patronymic).toString();
  }

  public ClientDot() {

  }

  public ClientDot(ClientToSave clientToSave) {
    this.id = clientToSave.id;
    this.name = clientToSave.name;
    this.surname = clientToSave.surname;
    this.gender = clientToSave.gender;
    this.patronymic = clientToSave.patronymic;
    this.birthDate = clientToSave.birthDate;
    this.charm = clientToSave.charm;
  }

  public ClientToSave toClientToSave() {
    ClientToSave clientToSave = new ClientToSave();
    clientToSave.id = id;
    clientToSave.name = name;
    clientToSave.surname = surname;
    clientToSave.patronymic = patronymic;
    clientToSave.birthDate = birthDate;
    clientToSave.charm = charm;
    clientToSave.gender = gender;
    return clientToSave;
  }


  public ClientRecord toClientRecord() {
    ClientRecord clientRecord = new ClientRecord();
    clientRecord.id = this.id;
    clientRecord.name = this.name;
    clientRecord.surname = this.surname;
    clientRecord.patronymic = this.patronymic;
    clientRecord.charm = this.charm;
    clientRecord.age = AgeCalculator.calculateAge(this.birthDate);

    return clientRecord;
  }

  public ClientDetail toClientForm() {
    ClientDetail clientDetail = new ClientDetail();
    clientDetail.id = this.id;
    clientDetail.name = this.name;
    clientDetail.surname = this.surname;
    clientDetail.patronymic = this.patronymic;
    clientDetail.charm = this.charm;
    clientDetail.birthDate = this.birthDate;
    clientDetail.gender = gender;
    return clientDetail;
  }


}


