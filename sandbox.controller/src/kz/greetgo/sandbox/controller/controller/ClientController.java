package kz.greetgo.sandbox.controller.controller;

import kz.greetgo.depinject.core.Bean;
import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.mvc.annotations.*;
import kz.greetgo.sandbox.controller.model.*;
import kz.greetgo.sandbox.controller.register.ClientRegister;
import kz.greetgo.sandbox.controller.security.NoSecurity;
import kz.greetgo.sandbox.controller.util.Controller;

import java.util.List;

@Bean
@Mapping("/client")
public class ClientController implements Controller {

    public BeanGetter<ClientRegister> clientRegister;

    @ToJson
    @Mapping("/clientsInfo/{pageID}/{filterStr}/{sortBy}/{sortOrder}")
    public ClientToReturn filteredClients(@ParPath("pageID") String pageID, @ParPath("filterStr") String filterStr,
            @ParPath("sortBy") String sortBy, @ParPath("sortOrder") String sortOrder) {
        return clientRegister.get().getFilteredClientsInfo(pageID, filterStr, sortBy, sortOrder);
    }

    @ToJson
    @Mapping("/addNewClient")
    public ClientRecord addNewClient(@Par("clientToSave") @Json ClientToSave clientToSave) {
        return clientRegister.get().addNewClient(clientToSave);
    }

    @ToJson
    @Mapping("/updateClient")
    public ClientRecord updateClient(@Par("clientToSave") @Json ClientToSave clientToSave) {
        return clientRegister.get().updateClient(clientToSave);
    }

    @AsIs
    @NoSecurity
    @Mapping("/removeClient")
    public String removeClient(@Par("clientID") String clientID) {
        return clientRegister.get().removeClient(clientID);
    }

    @ToJson
    @Mapping("/clientDetails/{clientID}")
    public ClientDetails getEditableClientInfo(@ParPath("clientID") String clientID) {
        return clientRegister.get().getEditableClientInfo(clientID);
    }

    @ToJson
    @Mapping("/charms")
    public List<Charm> getCharms() {
        return clientRegister.get().getCharms();
    }
}
