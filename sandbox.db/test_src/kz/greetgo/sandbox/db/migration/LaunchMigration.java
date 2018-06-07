package kz.greetgo.sandbox.db.migration;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import kz.greetgo.util.RND;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class LaunchMigration {
  static Connection connection;
  static List<String> frsFilePath = new ArrayList<>();
  static List<String> ciaFilePath = new ArrayList<>();

  private static void createDbConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    connection = DriverManager.getConnection(
      "jdbc:postgresql://localhost:5432/s_sandbox",
      "s_sandbox",
      "password"
    );
  }

  private static void createSSHConnection() throws Exception {
    String user = "seyit";
    String password = "111";
    String host = "192.168.11.44";
    int port = 22;
    String remoteDir = "/home/seyit/Downloads/source";

    try {
      JSch jsch = new JSch();
      Session session = jsch.getSession(user, host, port);
      session.setPassword(password);
      session.setConfig("StrictHostKeyChecking", "no");
      session.connect();

      ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
      sftpChannel.connect();

      String path = remoteDir + "/CIA/*tar.bz2*";
      Vector<ChannelSftp.LsEntry> files = sftpChannel.ls(path);

      for(ChannelSftp.LsEntry file : files) {
        path = remoteDir + "/CIA/" + file.getFilename();
        String newPath = path.substring(0, path.length() - 5);
        sftpChannel.rename(path, newPath);
      }

      String path2 = remoteDir + "/FRS/*tar.bz2*";
      Vector<ChannelSftp.LsEntry> files2 = sftpChannel.ls(path2);

      for(ChannelSftp.LsEntry file : files2) {
        path = remoteDir + "/FRS/" + file.getFilename();
        String newPath = path.substring(0, path.length() - 5);
        sftpChannel.rename(path, newPath);
      }

      String str = RND.str(5);
      for(ChannelSftp.LsEntry file : files) {
        path = remoteDir + "/" + file.getFilename();
        sftpChannel.rename(path, path + str);
      }

      for (ChannelSftp.LsEntry file : files) {

        path = remoteDir + "/" + file.getFilename() + str;

        InputStream in = sftpChannel.get(path);
        BZip2CompressorInputStream bzin = new BZip2CompressorInputStream(in);

        File out = new File("/Users/sanzharburumbay/Documents/Greetgo_Internship/greetgo.sandbox");

        try (TarArchiveInputStream fin = new TarArchiveInputStream(bzin)) {

          TarArchiveEntry entry;

          while ((entry = fin.getNextTarEntry()) != null) {
            if (entry.isDirectory()) {
              continue;
            }

            if (entry.getName().contains("xml")) {
              ciaFilePath.add(entry.getName());

              File ciaFile = new File(out, entry.getName());
              IOUtils.copy(fin, new FileOutputStream(ciaFile));
            }
            if (entry.getName().contains("json_row")) {
              frsFilePath.add(entry.getName());

              File frsFile = new File(out, entry.getName());
              IOUtils.copy(fin, new FileOutputStream(frsFile));
            }
          }
        }
      }

      session.disconnect();

    } catch (JSchException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    createDbConnection();

    createSSHConnection();

    final File file = new File("build/__migration__");
    file.getParentFile().mkdirs();
    file.createNewFile();

    System.out.println("To stop next migration portion delete file " + file);
    System.out.println("To stop next migration portion delete file " + file);
    System.out.println("To stop next migration portion delete file " + file);

//    try (Migration migration = new Migration(connection, frsFilePath, ciaFilePath)) {
//
//      migration.portionSize = 10_000_000;
//      migration.uploadMaxBatchSize = 80_000;
//      migration.downloadMaxBatchSize = 80_000;
//
//      while (true) {
//        int count = migration.execute();
//        if (count == 0) break;
//        if (count > 0) break;
//        if (!file.exists()) break;
//        System.out.println("Migrated " + count + " records");
//        System.out.println("------------------------------------------------------------------");
//        System.out.println("------------------------------------------------------------------");
//        System.out.println("------------------------------------------------------------------");
//        System.out.println("------------------------------------------------------------------");
//      }
//    }

    file.delete();

    System.out.println("Finish migration");
  }
}
