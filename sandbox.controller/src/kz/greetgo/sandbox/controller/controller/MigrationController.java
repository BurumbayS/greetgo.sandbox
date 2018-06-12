package kz.greetgo.sandbox.controller.controller;

import kz.greetgo.depinject.core.Bean;
import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.mvc.annotations.AsIs;
import kz.greetgo.mvc.annotations.Mapping;
import kz.greetgo.sandbox.controller.register.MigrationControllerInterface;
import kz.greetgo.sandbox.controller.security.NoSecurity;
import kz.greetgo.sandbox.controller.util.Controller;

@Bean
@Mapping("/migration")
public class MigrationController implements Controller{

  public BeanGetter<MigrationControllerInterface> migrationController;

  @AsIs
  @NoSecurity
  @Mapping("")
  public void runMigration() throws Exception { migrationController.get().runMigration(); }
}
