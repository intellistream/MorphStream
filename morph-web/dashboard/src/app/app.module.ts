import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppComponent } from './app.component';
import { NZ_I18N } from 'ng-zorro-antd/i18n';
import { en_US } from 'ng-zorro-antd/i18n';
import {NgOptimizedImage, registerLocaleData} from '@angular/common';
import en from '@angular/common/locales/en';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';
import { HttpClientModule } from '@angular/common/http';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { AppRoutingModule } from './app-routing.module';
import { IconsProviderModule } from './icons-provider.module';
import { NzLayoutModule } from 'ng-zorro-antd/layout';
import { NzMenuModule } from 'ng-zorro-antd/menu';
import { OverviewComponent } from "./pages/overview/overview.component";
import { HeaderComponent } from "./common/layout/header/header.component";
import { FooterComponent } from "./common/layout/footer/footer.component";
import { ApplicationCardComponent } from "./snippets/application-card/application-card.component";
import { NzGridModule } from "ng-zorro-antd/grid";
import { ScrollWrapperComponent } from "./snippets/scroll-wrapper/scroll-wrapper.component";
import { NzButtonModule } from "ng-zorro-antd/button";
import { JobInformationComponent } from "./pages/application-information/job-information.component";
import { NzBreadCrumbModule } from "ng-zorro-antd/breadcrumb";
import { NzProgressModule } from "ng-zorro-antd/progress";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzModalModule } from "ng-zorro-antd/modal";
import { FinishedStatisticsBoardComponent } from "./snippets/finished-statistics-board/finished-statistics-board.component";
import { SubmitNewJobComponent } from "./pages/submit-new-job/submit-new-job.component";
import { NzTabsModule } from "ng-zorro-antd/tabs";
import { NzDescriptionsModule } from "ng-zorro-antd/descriptions";
import {NzDividerModule} from "ng-zorro-antd/divider";
import {NzTableModule} from "ng-zorro-antd/table";
import {NzUploadModule} from "ng-zorro-antd/upload";
import {NzFormModule} from "ng-zorro-antd/form";
import {NzSelectModule} from "ng-zorro-antd/select";
import {NzListModule} from "ng-zorro-antd/list";
import {CdkFixedSizeVirtualScroll, CdkVirtualForOf, CdkVirtualScrollViewport} from "@angular/cdk/scrolling";
import {NgxChartsModule} from "@swimlane/ngx-charts";
import {NzCascaderModule} from "ng-zorro-antd/cascader";
import {CodeEditorComponent} from "./pages/code-editor/code-editor.component";
import {CodemirrorModule} from "@ctrl/ngx-codemirror";
import {NzCollapseModule} from "ng-zorro-antd/collapse";
import { NzGraphModule } from 'ng-zorro-antd/graph';
import {NzPopoverModule} from "ng-zorro-antd/popover";
import {NzMessageModule} from "ng-zorro-antd/message";
import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor';
import {NzInputModule} from "ng-zorro-antd/input";
import {NzInputNumberModule} from "ng-zorro-antd/input-number";
import {NzSwitchModule} from "ng-zorro-antd/switch";

registerLocaleData(en);

@NgModule({
  declarations: [
    AppComponent,
    OverviewComponent,
    HeaderComponent,
    FooterComponent,
    ApplicationCardComponent,
    ScrollWrapperComponent,
    JobInformationComponent,
    FinishedStatisticsBoardComponent,
    SubmitNewJobComponent,
    CodeEditorComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpClientModule,
    BrowserAnimationsModule,
    AppRoutingModule,
    IconsProviderModule,
    NzLayoutModule,
    NzMenuModule,
    NgOptimizedImage,
    NzGridModule,
    NzButtonModule,
    NzBreadCrumbModule,
    NzProgressModule,
    NzCardModule,
    NzModalModule,
    NzTabsModule,
    NzDescriptionsModule,
    NzDividerModule,
    NzTableModule,
    NzUploadModule,
    NzFormModule,
    ReactiveFormsModule,
    NzSelectModule,
    NzListModule,
    CdkVirtualForOf,
    CdkVirtualScrollViewport,
    CdkFixedSizeVirtualScroll,
    NgxChartsModule,
    NzCascaderModule,
    CodemirrorModule,
    NzCollapseModule,
    NzGraphModule,
    NzPopoverModule,
    NzMessageModule,
    NzCodeEditorModule,
    NzInputModule,
    NzInputNumberModule,
    NzSwitchModule
  ],
  providers: [
    { provide: NZ_I18N, useValue: en_US }
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
