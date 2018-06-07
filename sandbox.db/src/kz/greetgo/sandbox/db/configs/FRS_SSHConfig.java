package kz.greetgo.sandbox.db.configs;

import kz.greetgo.conf.hot.DefaultStrValue;
import kz.greetgo.conf.hot.Description;

@Description("Параметры доступа к SSH директории")
public interface FRS_SSHConfig {
  @Description("SSH директория")
  @DefaultStrValue("/home/seyit/Downloads/source/FRS")
  String dir();

  @Description("HOST SSH директории")
  @DefaultStrValue("192.168.11.44")
  String host();

  @Description("Порт SSH директории")
  @DefaultStrValue("22")
  String port();

  @Description("Пользователь для доступа к SSH директории")
  @DefaultStrValue("seyit")
  String username();

  @Description("Пароль для доступа к SSH директории")
  @DefaultStrValue("111")
  String password();
}
