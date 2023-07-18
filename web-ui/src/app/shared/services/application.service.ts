import { Injectable } from "@angular/core";
import {Application} from "../../model/application";

@Injectable({
  providedIn: 'root'
})
export class ApplicationService {
  private currentApplication!: Application;

  setCurrentApplication(application: Application) {
    this.currentApplication = application;
  }

  getCurrentApplication() {
    return this.currentApplication;
  }
}
