import { Component } from '@angular/core';
import {Router} from "@angular/router";
import {ApplicationService} from "../../../shared/services/application.service";
import {Application} from "../../../model/Application";

@Component({
  selector: 'app-finished-applications',
  templateUrl: './finished-applications.component.html',
  styleUrls: ['./finished-applications.component.less']
})
export class FinishedApplicationsComponent {
  constructor(private router: Router, private applicationService: ApplicationService) {
  }

  completedApplications: Application[] = [];

  navigateToAppDetails(application: Application) {
    this.applicationService.setCurrentApplication(application);
    this.router.navigate(['overview/application-details']);
  }
}
