import { Component } from '@angular/core';
import {Router} from "@angular/router";
import {ApplicationService} from "../../../shared/services/application.service";
import {Application} from "../../../model/Application";

@Component({
  selector: 'app-pocessing-applications',
  templateUrl: './processing-applications.component.html',
  styleUrls: ['./processing-applications.component.less']
})
export class ProcessingApplicationsComponent {
  constructor(private router: Router, private applicationService: ApplicationService) {
  }

  // Data for testing
  runningApplications: Application[] = [];

  navigateToAppDetails(application: Application) {
    this.applicationService.setCurrentApplication(application);
    this.router.navigate(['overview/application-details']);
  }
}
