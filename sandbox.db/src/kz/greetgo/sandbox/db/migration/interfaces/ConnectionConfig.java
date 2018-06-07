package kz.greetgo.sandbox.db.migration.interfaces;

//TODO: удали, если не используешь
public interface ConnectionConfig {
  String url();

  String user();

  String password();
}
