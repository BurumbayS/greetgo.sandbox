package kz.greetgo.sandbox.db.migration.core;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import kz.greetgo.depinject.core.Bean;
import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.mvc.annotations.AsIs;
import kz.greetgo.mvc.annotations.Mapping;
import kz.greetgo.sandbox.controller.report.MigrationSQLReport;
import kz.greetgo.sandbox.controller.security.NoSecurity;
import kz.greetgo.sandbox.controller.util.Controller;
import kz.greetgo.sandbox.db.configs.CIA_SSHConfig;
import kz.greetgo.sandbox.db.configs.DbConfig;
import kz.greetgo.sandbox.db.configs.FRS_SSHConfig;
import kz.greetgo.sandbox.db.util.App;
import kz.greetgo.util.RND;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

@Bean
@Mapping("/migration")
//TODO: контроллер миграции отличается от контроллеров типа (ClientController).
// Контроллер миграции включает бизнес логику. Тебе нужно создать ещё один контроллер в модуле Controller (где лежит ClientController)
// и перенести туда маппинг
public class MigrationController implements Controller, Closeable {

  public BeanGetter<DbConfig> postgresDbConfig;
  public BeanGetter<CIA_SSHConfig> cia_SSHConfig;
  public BeanGetter<FRS_SSHConfig> frs_SSHConfig;

  private Connection connection;
  private Session cia_session, frs_session;

  private final int MAX_BATH_SIZE = 80_000;

  private void createDbConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    connection = DriverManager.getConnection(
      postgresDbConfig.get().url(),
      postgresDbConfig.get().username(),
      postgresDbConfig.get().password()
    );
  }

  private void createSSHConnection() throws Exception {
    String user = cia_SSHConfig.get().username();
    String password = cia_SSHConfig.get().password();
    String host = cia_SSHConfig.get().host();
    int port = Integer.valueOf(cia_SSHConfig.get().port());

    cia_session = getSession(user, password, host, port);

    user = frs_SSHConfig.get().username();
    password = frs_SSHConfig.get().password();
    host = frs_SSHConfig.get().host();
    port = Integer.valueOf(frs_SSHConfig.get().port());

    frs_session = getSession(user, password, host, port);
  }

  private Session getSession(String user, String password, String host, int port) throws Exception {
    try {
      JSch jsch = new JSch();
      Session session = jsch.getSession(user, host, port);
      session.setPassword(password);
      session.setConfig("StrictHostKeyChecking", "no");

      return session;

    } catch (JSchException e) {
      throw e;
    }
  }

  @AsIs
  @NoSecurity
  @Mapping("")
  public void runMigration() throws Exception {
    createDbConnection();

    createSSHConnection();

    loadAndMigrateData();
  }

  private void loadAndMigrateData() throws Exception {

    cia_session.connect();
    frs_session.connect();

    do {
      boolean migratedFromCia = false;
      boolean migratedFromFrs = false;

      migratedFromCia = migrateFromCia();

      migratedFromFrs = migrateFromFrs();

      if (!migratedFromCia && !migratedFromFrs) { return; }

    } while (true);
  }

  private boolean migrateFromCia() throws Exception {
    String remoteDir = cia_SSHConfig.get().dir();

    //TODO: Не разъеденил и не вышел с подключения
    ChannelSftp sftpChannel = (ChannelSftp) cia_session.openChannel("sftp");
    sftpChannel.connect();

    String path = remoteDir + "/*xml.tar.bz2";
    Vector<ChannelSftp.LsEntry> files = sftpChannel.ls(path);

    if (files.size() < 1) { return false; }

    String str = RND.str(5);
    path = remoteDir + "/" + files.get(0).getFilename();
    sftpChannel.rename(path, path + str);

    ChannelSftp.LsEntry file = files.get(0);

    path = remoteDir + "/" + file.getFilename() + str;

    //TODO: не закрыт
    InputStream in = sftpChannel.get(path);
    //TODO: не закрыт
    BZip2CompressorInputStream bzin = new BZip2CompressorInputStream(in);

    File out = new File("/Users/sanzharburumbay/Documents/Greetgo_Internship/greetgo.sandbox");

//    String dir = remoteDir.substring(0, remoteDir.length() - 4);
//    Vector<ChannelSftp.LsEntry> fls = sftpChannel.ls(dir + "/CiaError*");
//    if (fls.size() == 0) {
//      sftpChannel.mkdir(dir + "/CiaErrors");
//    }
//    OutputStream errorOutStream = sftpChannel.put(dir + "/CiaErrors/errors.txt");
//    OutputStreamWriter errorWriter = new OutputStreamWriter(errorOutStream);
//    errorWriter.write("Hello");
//    errorWriter.close();

//    sftpChannel.cd(remoteDir);
//    File f = new File("test.xlsx");
//    FileInputStream fis = new FileInputStream(f);
//    sftpChannel.put(fis, f.getName());
//    OutputStream reportOuts = sftpChannel.put(remoteDir + "/sqlReport.xlsx");
    File fl = new File(App.appDir() + "/errors.txt");
    //TODO: не закрыт
    FileOutputStream errorOut = new FileOutputStream(fl);

    try (TarArchiveInputStream fin = new TarArchiveInputStream(bzin)) {

      TarArchiveEntry entry;

      while ((entry = fin.getNextTarEntry()) != null) {
        if (entry.isDirectory()) {
          continue;
        }

        File ciaFile = new File(out, entry.getName());
        IOUtils.copy(fin, new FileOutputStream(ciaFile));

        InputStream inputStream = new FileInputStream(ciaFile);

        MigrationWorkerCIA migrationWorkerCIA = new MigrationWorkerCIA();
        migrationWorkerCIA.migrate(connection, inputStream, errorOut, MAX_BATH_SIZE);

        generateSQLReport(migrationWorkerCIA.getSqlRequests(), "CiaSqlRequests.xlsx");

        return true;
      }
    }

    return false;
  }

  private boolean migrateFromFrs() throws Exception {
    String remoteDir = frs_SSHConfig.get().dir();

    ChannelSftp sftpChannel = (ChannelSftp) frs_session.openChannel("sftp");
    sftpChannel.connect();

    String path = remoteDir + "/*txt.tar.bz2";
    Vector<ChannelSftp.LsEntry> files = sftpChannel.ls(path);

    if (files.size() < 1) { return false; }

    String str = RND.str(5);
    path = remoteDir + "/" + files.get(0).getFilename();
    sftpChannel.rename(path, path + str);

    ChannelSftp.LsEntry file = files.get(0);

    path = remoteDir + "/" + file.getFilename() + str;

    InputStream in = sftpChannel.get(path);
    BZip2CompressorInputStream bzin = new BZip2CompressorInputStream(in);

    File out = new File("/Users/sanzharburumbay/Documents/Greetgo_Internship/greetgo.sandbox");

//    sftpChannel.mkdir(remoteDir.substring(0, remoteDir.length() - 4) + "/FrsErrors");
//    OutputStream errorOutStream = sftpChannel.put(remoteDir.substring(0, remoteDir.length() - 4) + "/FrsErrors/errors.txt");
//    OutputStreamWriter errorWriter = new OutputStreamWriter(errorOutStream);
//    errorWriter.write("Hello");
//    errorWriter.close();

//    OutputStream reportOuts = sftpChannel.put(remoteDir + "/sqlReport.xlsx");

    File fl = new File(App.appDir() + "/errors.txt");
    FileOutputStream errorOut = new FileOutputStream(fl);

    try (TarArchiveInputStream fin = new TarArchiveInputStream(bzin)) {

      TarArchiveEntry entry;

      while ((entry = fin.getNextTarEntry()) != null) {
        if (entry.isDirectory()) {
          continue;
        }

        File frsFile = new File(out, entry.getName());
        IOUtils.copy(fin, new FileOutputStream(frsFile));

        InputStream inputStream = new FileInputStream(frsFile);

        MigrationWorkerFRS migrationWorkerFRS = new MigrationWorkerFRS();
        migrationWorkerFRS.migrate(connection, inputStream, errorOut, MAX_BATH_SIZE);

        generateSQLReport(migrationWorkerFRS.getSqlRequests(), "FrsSqlRequests.xlsx");

        return true;
      }
    }

    return false;
  }

  private void generateSQLReport(Map<String, String> sqlRequests, String fileName) throws Exception {
    File file = new File(App.appDir() + "/" + fileName);

    //TODO: не закрыт стрим. В билетах есть решение как правильно заркывать
    //Посмотри try-with-resources
    FileOutputStream out = new FileOutputStream(file);

    MigrationSQLReport report = new MigrationSQLReport(out);

    report.start("Список SQL запросов");

    List<String> keys = new ArrayList<>();
    List<String> values = new ArrayList<>();

    for (String key : sqlRequests.keySet()) {
      keys.add(key);
      values.add(sqlRequests.get(key));
    }

    for (int i = sqlRequests.size() - 1; i >= 0; i--) {
      report.append(sqlRequests.size() - i, values.get(i), keys.get(i));
    }

    report.finish();
  }

  @Override
  public void close() throws IOException {
    cia_session.disconnect();
    frs_session.disconnect();

    if (this.connection != null) {
      try {
        this.connection.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      this.connection = null;
    }
  }
}
