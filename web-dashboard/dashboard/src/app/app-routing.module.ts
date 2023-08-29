import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import {OverviewComponent} from "./pages/overview/overview.component";
import {FinishedApplicationsComponent} from "./pages/applications/finished-applications/finished-applications.component";
import {ProcessingApplicationsComponent} from "./pages/applications/processing-applications/processing-applications.component";
import {ApplicationInformationComponent} from "./pages/application-information/application-information.component";

const routes: Routes = [
  // Home
  { path: '', pathMatch: 'full', redirectTo: '/overview' },
  { path: 'overview/application-details', component: ApplicationInformationComponent },
  { path: 'overview', component: OverviewComponent },
  { path: 'applications/finished-applications', component: FinishedApplicationsComponent},
  { path: 'applications/processing-applications', component: ProcessingApplicationsComponent}
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
