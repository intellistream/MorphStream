import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import {OverviewComponent} from "./pages/overview/overview.component";
import {FinishedApplicationsComponent} from "./pages/applications/finished-applications/finished-applications.component";
import {ProcessingApplicationsComponent} from "./pages/applications/processing-applications/processing-applications.component";
import {JobInformationComponent} from "./pages/application-information/job-information.component";
import {SubmitNewJobComponent} from "./pages/submit-new-job/submit-new-job.component";

const routes: Routes = [
  // Home
  { path: '', pathMatch: 'full', redirectTo: '/overview' },
  { path: 'overview/application-details', component: JobInformationComponent },
  { path: 'overview', component: OverviewComponent },
  { path: 'applications/finished-applications', component: FinishedApplicationsComponent },
  { path: 'applications/processing-applications', component: ProcessingApplicationsComponent },
  { path: 'submit-new-job', component: SubmitNewJobComponent }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
