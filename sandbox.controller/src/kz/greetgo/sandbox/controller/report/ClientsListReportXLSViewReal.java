package kz.greetgo.sandbox.controller.report;

import kz.greetgo.msoffice.xlsx.gen.Sheet;
import kz.greetgo.msoffice.xlsx.gen.Xlsx;
import kz.greetgo.sandbox.controller.report.model.ClientListRow;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Date;

public class ClientsListReportXLSViewReal implements ClientsListReportView {

  private final OutputStream out;
  private Xlsx xlsx;
  private Sheet sheet;

  public ClientsListReportXLSViewReal(OutputStream out) {
    this.out = out;
  }

  @Override
  public void start(String title) {
    xlsx = new Xlsx();

    sheet = xlsx.newSheet(true);
    sheet.setScaleByWidth();

    sheet.row().start();
    sheet.cellStr(1, title);
    sheet.row().finish();

    sheet.skipRow();

    sheet.row().start();
    sheet.cellStr(1, "#");
    sheet.cellStr(2, "ФИО");
    sheet.cellStr(3, "Характер");
    sheet.cellStr(4, "Возраст");
    sheet.cellStr(5, "Общий остаток");
    sheet.cellStr(6, "Макс. остаток");
    sheet.cellStr(7, "Мин. остаток");
    sheet.row().finish();
  }

  @Override
  public void append(ClientListRow clientListRow) {
    sheet.row().start();
    sheet.cellInt(1, clientListRow.no);
    sheet.setScaleByWidth();
    sheet.cellStr(2, clientListRow.fio);
    sheet.setScaleByWidth();
    sheet.cellStr(3, clientListRow.charm);
    sheet.setScaleByWidth();
    sheet.cellInt(4, clientListRow.age);
    sheet.setScaleByWidth();
    sheet.cellDouble(5, clientListRow.totalCash);
    sheet.setScaleByWidth();
    sheet.cellDouble(6, clientListRow.maxCash);
    sheet.setScaleByWidth();
    sheet.cellDouble(7, clientListRow.minCash);
    sheet.row().finish();
  }

  @Override
  public void finish(String userName) {
    sheet.skipRow();

    sheet.row().start();
    sheet.cellStr(1, "Сформирован: " + userName);
    sheet.cellDMY(2, new Date());
    sheet.row().finish();

    xlsx.complete(out);
  }

  public static void main(String args[]) throws Exception {
    String home = System.getProperty("user.home");

    File file = new File("build/out_files/test.xlsx");
    file.getParentFile().mkdirs();
    OutputStream outf = new FileOutputStream(file);

    ClientsListReportXLSViewReal reportView = new ClientsListReportXLSViewReal(outf);

    reportView.start("Список клиентов");

    for (int i = 1; i <= 5; i++) {
      ClientListRow row = new ClientListRow();
      row.no = i;
      row.fio = "Asdas Aasdas Aasdasd " + i;
      row.age = 20 + i;
      row.charm = "charm" + i;
      row.totalCash = 30000 + i;
      row.maxCash = 40000 + i;
      row.minCash = 20000 + i;

      reportView.append(row);
    }

    reportView.finish("Бурумбай Санжар");

    System.out.println("OK");
  }
}