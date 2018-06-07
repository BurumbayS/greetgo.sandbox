package kz.greetgo.sandbox.db.beans.all;

import kz.greetgo.depinject.core.Bean;
import kz.greetgo.sandbox.db.configs.CIA_SSHConfig;
import kz.greetgo.sandbox.db.configs.DbConfig;
import kz.greetgo.sandbox.db.configs.FRS_SSHConfig;
import kz.greetgo.sandbox.db.util.LocalConfigFactory;

@Bean
public class AllConfigFactory extends LocalConfigFactory {

  @Bean
  public DbConfig createPostgresDbConfig() {
    return createConfig(DbConfig.class);
  }

  @Bean
  public CIA_SSHConfig createCiaSSHConfig() {
    return createConfig(CIA_SSHConfig.class);
  }

  @Bean
  public FRS_SSHConfig createFrsSSHConfig() {
    return createConfig(FRS_SSHConfig.class);
  }
}
