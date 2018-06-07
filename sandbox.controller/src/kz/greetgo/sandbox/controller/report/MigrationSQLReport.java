package kz.greetgo.sandbox.controller.report;

import kz.greetgo.msoffice.xlsx.gen.Sheet;
import kz.greetgo.msoffice.xlsx.gen.Xlsx;
import kz.greetgo.sandbox.controller.report.model.ClientListRow;

import java.io.OutputStream;
import java.util.Date;
import java.util.Map;

public class MigrationSQLReport {
  private final OutputStream out;
  private Xlsx xlsx;
  private Sheet sheet;

  public MigrationSQLReport(OutputStream out) { this.out = out; }

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
    sheet.cellStr(2, "SQL-запрос");
    sheet.cellStr(3, "Время вып.");
    sheet.row().finish();
  }

  public void append(int index, String sql, String executionTime) {
    sheet.row().start();
    sheet.cellInt(1, index);
    sheet.setScaleByWidth();
    sheet.cellStr(2, sql);
    sheet.setScaleByWidth();
    sheet.cellStr(3, executionTime);
    sheet.setScaleByWidth();
    sheet.row().finish();
  }

  public void finish() {
    sheet.skipRow();

    sheet.row().start();
    sheet.cellDMY(1, new Date());
    sheet.row().finish();

    xlsx.complete(out);
  }
}
