import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import {OverviewComponent} from "./pages/overview/overview.component";
import {JobInformationComponent} from "./pages/job-information/job-information.component";
import {SubmitNewJobComponent} from "./pages/submit-new-job/submit-new-job.component";
import {CodeEditorComponent} from "./pages/code-editor/code-editor.component";

const routes: Routes = [
  // Home
  { path: '', pathMatch: 'full', redirectTo: '/overview' },
  { path: 'job/:id', component: JobInformationComponent },
  // { path: 'overview/application-details', component: JobInformationComponent },
  { path: 'overview', component: OverviewComponent },
  // { path: 'applications/finished-applications', component: FinishedApplicationsComponent },
  // { path: 'applications/processing-applications', component: ProcessingApplicationsComponent },
  { path: 'submit-new-job', component: SubmitNewJobComponent },
  { path: 'code-editor', component: CodeEditorComponent }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
