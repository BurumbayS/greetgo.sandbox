package kz.greetgo.sandbox.db.report;

import kz.greetgo.msoffice.xlsx.gen.Sheet;
import kz.greetgo.sandbox.db.report.model.ClientListRow;
import kz.greetgo.msoffice.xlsx.gen.Xlsx;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Date;

public class ClientsListReportViewReal implements ClientsListReportView{

    private final OutputStream out;
    private Xlsx xlsx;
    private Sheet sheet;

    public ClientsListReportViewReal(OutputStream out) {
        this.out = out;
    }

    @Override
    public void start(String title) {
        xlsx = new Xlsx();

        sheet = xlsx.newSheet(true);

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
        sheet.cellStr(2, clientListRow.fio);
        sheet.cellStr(3, clientListRow.charm);
        sheet.cellInt(4, clientListRow.age);
        sheet.cellDouble(5, clientListRow.totalCash);
        sheet.cellDouble(6, clientListRow.maxCash);
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
        OutputStream outf = new FileOutputStream(new File("/Users/sanzharburumbay/Downloads/test.xlsx"));

        ClientsListReportViewReal reportView = new ClientsListReportViewReal(outf);

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
