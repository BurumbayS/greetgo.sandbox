package kz.greetgo.sandbox.controller.controller;

import kz.greetgo.depinject.core.Bean;
import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.mvc.annotations.AsIs;
import kz.greetgo.mvc.annotations.Mapping;
import kz.greetgo.sandbox.controller.security.NoSecurity;

@Bean
@Mapping("/migration")
public class MigrationController {

  public BeanGetter<MigrationController> migrationController;

  @AsIs
  @NoSecurity
  @Mapping("")
  public void runMigration() { migrationController.get().runMigration(); }
}
