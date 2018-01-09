import {Component, EventEmitter, Output} from "@angular/core";
import {HttpService} from "../HttpService";
import {UserInfo} from "../../model/UserInfo";
import {PhoneType} from "../../model/PhoneType";

@Component({
  selector: 'main-form-component',
  template: require('./main-form-component.html'),
  styles: [require('./main-form-component.css')],
})

export class MainFormComponent {
  @Output() exit = new EventEmitter<void>();

  userInfo: UserInfo | null = null;
  loadUserInfoButtonEnabled: boolean = true;
  loadUserInfoError: string | null;

  constructor(private httpService: HttpService) {}

  loadUserInfoButtonClicked() {
    this.loadUserInfoButtonEnabled = false;
    this.loadUserInfoError = null;

    this.httpService.get("/auth/userInfo").toPromise().then(result => {
      this.userInfo = UserInfo.copy(result.json());
      let phoneType: PhoneType | null = this.userInfo.phoneType;
      console.log(phoneType);
    }, error => {
      console.log(error);
      this.loadUserInfoButtonEnabled = true;
      this.loadUserInfoError = error;
      this.userInfo = null;
    });
  }
}
