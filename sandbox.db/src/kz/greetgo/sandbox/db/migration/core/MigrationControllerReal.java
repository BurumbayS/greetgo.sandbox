package kz.greetgo.sandbox.db.migration.core;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import kz.greetgo.depinject.core.Bean;
import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.sandbox.controller.register.MigrationControllerInterface;
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
import java.sql.Statement;
import java.util.Vector;


//TODO: контроллер миграции отличается от контроллеров типа (ClientController).
// Контроллер миграции включает бизнес логику. Тебе нужно создать ещё один контроллер в модуле Controller (где лежит ClientController)
// и перенести туда маппинг
@Bean
public class MigrationControllerReal implements MigrationControllerInterface {

  public BeanGetter<DbConfig> postgresDbConfig;
  public BeanGetter<CIA_SSHConfig> cia_SSHConfig;
  public BeanGetter<FRS_SSHConfig> frs_SSHConfig;

  private Connection connection;
  private Session cia_session, frs_session;
  private String currentMigratedCiaFile, currentMigratedFrsFile;

  private final int MAX_BATH_SIZE = 80_000;

  private void createDbConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    connection = DriverManager.getConnection(
      postgresDbConfig.get().url(),
      postgresDbConfig.get().username(),
      postgresDbConfig.get().password()
    );
  }

  private Session getSession(String user, String password, String host, int port) throws Exception {
    JSch jsch = new JSch();
    Session session = jsch.getSession(user, host, port);
    session.setPassword(password);
    session.setConfig("StrictHostKeyChecking", "no");

    return session;

  }

  @SuppressWarnings("Duplicates")
  private void openCiaSession() throws Exception {
    String user = cia_SSHConfig.get().username();
    String password = cia_SSHConfig.get().password();
    String host = cia_SSHConfig.get().host();
    int port = Integer.valueOf(cia_SSHConfig.get().port());

    cia_session = getSession(user, password, host, port);
    cia_session.connect();
  }

  @SuppressWarnings("Duplicates")
  private void openFrsSession() throws Exception {
    String user = frs_SSHConfig.get().username();
    String password = frs_SSHConfig.get().password();
    String host = frs_SSHConfig.get().host();
    int port = Integer.valueOf(frs_SSHConfig.get().port());

    frs_session = getSession(user, password, host, port);
    frs_session.connect();
  }

  public void runMigration() throws Exception {

    createDbConnection();

    try {

      while (migrateOneCiaFile() || migrateOneFrsFile()) { }

      deleteTables();

    } finally {
      connection.close();
    }
  }

  private boolean migrateOneCiaFile() throws Exception {
    openCiaSession();

    try {

      return migrateOneCiaFileWithSession();

    } finally {
      closeCiaSession();
    }
  }

  private boolean migrateOneFrsFile() throws Exception {
    openFrsSession();

    try {

      return migrateOneFrsFileWithSession();

    } finally {
      closeFrsSession();
    }
  }

  private boolean migrateOneCiaFileWithSession() throws Exception {

    File ciaFile = downloadOneCiaFileToLocalTmpDir();
    if (ciaFile == null) return false;

    File errorFile = parseAndMigrateCiaFile(ciaFile);
    String remoteDir = cia_SSHConfig.get().dir();
    uploadErrorFile(cia_session, remoteDir, errorFile);

    doneFile(cia_session, cia_SSHConfig.get().dir(), currentMigratedCiaFile);

    return true;
  }

  private boolean migrateOneFrsFileWithSession() throws Exception {

    File frsFile = downloadOneFrsFileToLocalTmpDir();
    if (frsFile == null) return false;

    File errorFile = parseAndMigrateFrsFile(frsFile);
    String remoteDir = frs_SSHConfig.get().dir();
    uploadErrorFile(frs_session, remoteDir, errorFile);

    doneFile(frs_session, frs_SSHConfig.get().dir(), currentMigratedFrsFile);

    return true;
  }

  private File downloadOneCiaFileToLocalTmpDir() throws Exception {
    ChannelSftp sftpChannel = (ChannelSftp) cia_session.openChannel("sftp");

    try {

      sftpChannel.connect();

      String remoteDir = cia_SSHConfig.get().dir();
      String path = remoteDir + "/*xml.tar.bz2";
      Vector<ChannelSftp.LsEntry> files = sftpChannel.ls(path);

      if (files.size() < 1) { return null; }

      String str = RND.str(5);
      path = remoteDir + "/" + files.get(0).getFilename();
      sftpChannel.rename(path, path + str);

      currentMigratedCiaFile = files.get(0).getFilename() + str;

      ChannelSftp.LsEntry file = files.get(0);
      path = remoteDir + "/" + file.getFilename() + str;

      return getFileByPath(sftpChannel, path);

    } finally {

      sftpChannel.disconnect();
      sftpChannel.exit();

    }
  }

  private File downloadOneFrsFileToLocalTmpDir() throws Exception {
    ChannelSftp sftpChannel = (ChannelSftp) frs_session.openChannel("sftp");

    try {

      sftpChannel.connect();

      String remoteDir = frs_SSHConfig.get().dir();
      String path = remoteDir + "/*txt.tar.bz2";
      Vector<ChannelSftp.LsEntry> files = sftpChannel.ls(path);

      if (files.size() < 1) { return null; }

      String str = RND.str(5);
      path = remoteDir + "/" + files.get(0).getFilename();
      sftpChannel.rename(path, path + str);

      currentMigratedFrsFile = files.get(0).getFilename() + str;

      ChannelSftp.LsEntry file = files.get(0);
      path = remoteDir + "/" + file.getFilename() + str;

      return getFileByPath(sftpChannel, path);

    } finally {

      sftpChannel.disconnect();
      sftpChannel.exit();

    }
  }

  private File getFileByPath(ChannelSftp sftpChannel, String path) throws Exception {

    try (InputStream in = sftpChannel.get(path)) {

      try (BZip2CompressorInputStream bzin = new BZip2CompressorInputStream(in)) {

        try (TarArchiveInputStream fin = new TarArchiveInputStream(bzin)) {

          TarArchiveEntry entry;

          while ((entry = fin.getNextTarEntry()) != null) {
            if (entry.isDirectory()) {
              continue;
            }

            File out = new File(App.appDir());

            String fileName = entry.getName().substring(16, entry.getName().length());
            File file = new File(out, fileName);
            IOUtils.copy(fin, new FileOutputStream(file));

            return file;
          }
        }
      }
    }

    return null;
  }

  private File parseAndMigrateCiaFile(File ciaFile) throws Exception {

    File errorFile = new File(App.appDir() + "/ciaErrors.txt");

    try (FileOutputStream errorOutStream = new FileOutputStream(errorFile);
         FileInputStream ciaStream = new FileInputStream(ciaFile)) {

      parseAndMigrateCiaFileWithErrorStream(ciaStream, errorOutStream);
    }

    return errorFile;
  }

  private File parseAndMigrateFrsFile(File frsFile) throws Exception {

    File errorFile = new File(App.appDir() + "/frsErrors.txt");

    try (FileOutputStream errorOutStream = new FileOutputStream(errorFile);
         FileInputStream frsStream = new FileInputStream(frsFile)) {

      parseAndMigrateFrsFileWithErrorStream(frsStream, errorOutStream);
    }

    return errorFile;
  }

  private void parseAndMigrateFrsFileWithErrorStream(FileInputStream inputStream, FileOutputStream errorOutStream) throws Exception {

    MigrationWorkerFRS migrationWorkerFRS = new MigrationWorkerFRS(connection, inputStream, errorOutStream, MAX_BATH_SIZE);
    migrationWorkerFRS.migrate();

    File report = migrationWorkerFRS.generateSQLReport("FrsSqlReport.xlsx");
    uploadReportFile(frs_session, frs_SSHConfig.get().dir(), report);
  }

  private void parseAndMigrateCiaFileWithErrorStream(FileInputStream inputStream, FileOutputStream errorOutStream) throws Exception {

    MigrationWorkerCIA migrationWorkerCIA = new MigrationWorkerCIA(connection, inputStream, errorOutStream, MAX_BATH_SIZE);
    migrationWorkerCIA.migrate();

    File report = migrationWorkerCIA.generateSQLReport("CiaSqlReport.xlsx");
    uploadReportFile(cia_session, cia_SSHConfig.get().dir(), report);
  }

  private void uploadErrorFile(Session session, String remoteDir, File errorFile) throws Exception {
    ChannelSftp sftp = (ChannelSftp) session.openChannel("sftp");

    try {

      sftp.connect();

      try (InputStream inputStream = new FileInputStream(errorFile)) {

        sftp.put(inputStream, remoteDir + "/errors.txt");

      }

    } finally {
      sftp.disconnect();
      sftp.exit();
    }
  }

  private void uploadReportFile(Session session, String remoteDir, File reportFile) throws Exception {
    ChannelSftp sftp = (ChannelSftp) session.openChannel("sftp");

    try {

      sftp.connect();

      try (InputStream inputStream = new FileInputStream(reportFile)) {

        sftp.put(inputStream, remoteDir + "/sqlReport.xlsx");

      }

    } finally {
      sftp.disconnect();
      sftp.exit();
    }
  }

  private void doneFile(Session session, String remoteDir, String name) throws Exception {
    ChannelSftp sftpRead = (ChannelSftp) session.openChannel("sftp");
    ChannelSftp sftpWrite = (ChannelSftp) session.openChannel("sftp");

    try {

      sftpRead.connect();
      sftpWrite.connect();

      try {

        sftpRead.mkdir(remoteDir + "/done");

      } finally {

        InputStream from = sftpRead.get(remoteDir + "/" + name);
        sftpWrite.put(from, remoteDir + "/done/" + name);
        sftpRead.rm(remoteDir + "/" + name);

      }

    } finally {
      sftpRead.disconnect();
      sftpWrite.disconnect();

      sftpRead.exit();
      sftpWrite.exit();
    }
  }

  private void closeCiaSession() {
    cia_session.disconnect();
  }

  private void closeFrsSession() {
    frs_session.disconnect();
  }

  private void deleteTables() throws Exception {
    String sql = "CREATE OR REPLACE FUNCTION removeTables()\n" +
      "  RETURNS void\n" +
      "LANGUAGE plpgsql AS\n" +
      "$$\n" +
      "DECLARE row  record;\n" +
      "BEGIN\n" +
      "  FOR row IN\n" +
      "  SELECT\n" +
      "    table_schema,\n" +
      "    table_name\n" +
      "  FROM\n" +
      "    information_schema.tables\n" +
      "  WHERE\n" +
      "    table_name LIKE ('cia_migration%') or\n" +
      "    table_name LIKE ('frs_migration%')\n" +
      "  LOOP\n" +
      "    EXECUTE 'DROP TABLE ' || quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);\n" +
      "  END LOOP;\n" +
      "END;\n" +
      "$$;\n" +
      "\n" +
      "select removeTables()";

    try (Statement st = connection.createStatement()) {
      st.execute(sql);
    }
  }
}
