package kz.greetgo.sandbox.controller.register.report.client_list.font;

import java.io.InputStream;

public class FontFactory {
  public static String FONT_NAME_ROBOTO = "Roboto-Regular.ttf";

  public static InputStream getRoboto() {
    return FontFactory.class.getResourceAsStream(FONT_NAME_ROBOTO);
  }
}