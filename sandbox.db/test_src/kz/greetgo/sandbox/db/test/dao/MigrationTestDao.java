package kz.greetgo.sandbox.db.test.dao;

import kz.greetgo.sandbox.db.migration.model.AccountJSONRecord;
import kz.greetgo.sandbox.db.migration.model.ClientXMLRecord;
import kz.greetgo.sandbox.db.migration.model.TransactionJSONRecord;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface MigrationTestDao {

    @Select("Select table_name from information_schema.tables where table_name like 'cia_migration_%' or " +
            "table_name like 'frs_migration_%'")
    List<String> getCiaTableNames();

    @Select("Select cia_id from cia_migration_client_ where number = #{number}")
    String getCiaClientID(Long number);

    @Select("Select COUNT(cia_id) from cia_migration_client_ where cia_id = #{cia_id} and " +
            "name = #{name} and surname = #{surname} and patronymic = #{patronymic} and " +
            "gender = #{gender} and charm = #{charm} and birth_date = #{birthDate} and " +
            "fStreet = #{fStreet} and fHouse = #{fHouse} and fFlat = #{fFlat} and " +
            "rStreet = #{rStreet} and rHouse = #{rHouse} and rFlat = #{rFlat}")
    Integer getCiaClient(ClientXMLRecord clientXMLRecord);

    @Select("Select cia_id from cia_migration_phone_ where " +
            "cia_id = #{cia_id} and number = #{number} and phoneType = #{phoneType}")
    String getCiaPhone(@Param("cia_id") String id, @Param("number") String number, @Param("phoneType") String phoneType);

    @Select("Select number from frs_migration_transaction_ where number = #{number}")
    Long getTransactionCiaNumber(Long number);

    @Select("Select COUNT(number) from frs_migration_transaction_ where account_number = #{account_number} and " +
            "money = #{money} and transaction_type = #{transaction_type} and finished_at = #{finished_at}")
    Integer getCiaTransaction(TransactionJSONRecord transactionJSONRecord);

    @Select("Select number from frs_migration_account_ where number = #{number}")
    Long getAccountCiaNumber(Long number);

    @Select("Select COUNT(number) from frs_migration_account_ where account_number = #{account_number} and " +
            "registered_at = #{registered_at} and client_cia_id = #{client_id}")
    Integer getCiaAccount(AccountJSONRecord accountJSONRecord);

    @Select("Select status from cia_migration_client_ where " +
            "number = #{number}")
    Integer getCiaClientStatus(Long number);

    @Select("Select status from frs_migration_transaction_ where number = #{number}")
    Integer getCiaTransactionStatus(Long number);

    @Select("Select status from frs_migration_account_ where number = #{number}")
    Integer getCiaAccountStatus(Long number);

    @Select("Select id from tmp_clients where name = #{name} and surname = #{surname} and " +
            "gender = #{gender} and charm = #{charm} and birth_date = #{birthDate}")
    String getClient(ClientXMLRecord clientXMLRecord);

    @Select("Select id from tmp_clients where cia_id = #{cia_id}")
    String getClientID(@Param("cia_id") String cia_id);

    @Select("Select cia_id from tmp_clients where id = #{id}")
    String getClientCiaID(@Param("id") String id);

    @Select("Select number from tmp_phones where number = #{number} and phoneType = #{phoneType} and " +
      "cia_id = #{cia_id}")
    String getPhone(@Param("number") String number, @Param("phoneType") String phoneType, @Param("cia_id") String cia_id);

    @Select("Select client_id from tmp_accounts where number = #{account_number} and registered_at = #{registered_at}")
    String getAccountClientID(AccountJSONRecord accountJSONRecord);

    @Select("Select number from tmp_accounts where id = #{id}")
    String getAccountNumber(@Param("id") Integer id);

    @Select("Select id from tmp_accounts where number = #{number}")
    Integer getAccountID(@Param("number") String number);

    @Select("Select account_id from tmp_transactions where money = #{money} and finished_at = #{finished_at}")
    Integer getTransactionAccountID(TransactionJSONRecord transactionJSONRecord);

    @Select("Select transaction_type_id from tmp_transactions where money = #{money} and finished_at = #{finished_at}")
    Integer getTransactionTypeID(TransactionJSONRecord transactionJSONRecord);

    @Select("Select name from tmp_transaction_types where id = #{id}")
    String getTransactionTypeName(@Param("id") Integer id);
}
