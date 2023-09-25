import { Injectable } from "@angular/core";
import {BasicApplication} from "../../model/BasicApplication";

@Injectable({
  providedIn: 'root'
})
export class ApplicationService {
  private currentApplication!: BasicApplication;

  setCurrentApplication(application: BasicApplication) {
    this.currentApplication = application;
  }

  getCurrentApplication() {
    return this.currentApplication;
  }
}
