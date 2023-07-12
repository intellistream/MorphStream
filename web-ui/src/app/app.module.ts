import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppComponent } from './app.component';
import { NZ_I18N } from 'ng-zorro-antd/i18n';
import { en_US } from 'ng-zorro-antd/i18n';
import {NgOptimizedImage, registerLocaleData} from '@angular/common';
import en from '@angular/common/locales/en';
import { FormsModule } from '@angular/forms';
import { HttpClientModule } from '@angular/common/http';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { AppRoutingModule } from './app-routing.module';
import { IconsProviderModule } from './icons-provider.module';
import { NzLayoutModule } from 'ng-zorro-antd/layout';
import { NzMenuModule } from 'ng-zorro-antd/menu';
import { OverviewComponent } from "./pages/overview/overview.component";
import { HeaderComponent } from "./common/layout/header/header.component";
import { FooterComponent } from "./common/layout/footer/footer.component";
import { ProcessingApplicationsComponent } from "./pages/applications/processing-applications/processing-applications.component";
import { FinishedApplicationsComponent } from "./pages/applications/finished-applications/finished-applications.component";
import { ApplicationCardComponent } from "./snippets/application-card/application-card.component";
import { NzGridModule } from "ng-zorro-antd/grid";
import { ScrollWrapperComponent } from "./snippets/scroll-wrapper/scroll-wrapper.component";
import { NzButtonModule } from "ng-zorro-antd/button";
import { ApplicationInformationComponent } from "./pages/application-information/application-information.component";
import { ProcessingApplicationListElementComponent } from "./pages/applications/processing-applications/processing-application-list-element/processing-application-list-element.component";
import { FinishedApplicationListElementComponent } from "./pages/applications/finished-applications/finished-application-list-element/finished-application-list-element.component";
import {NzBreadCrumbModule} from "ng-zorro-antd/breadcrumb";
import {NzProgressModule} from "ng-zorro-antd/progress";
import {ApplicationBoardComponent} from "./snippets/application-board/application-board.component";
import {InfoScrollWrapperComponent} from "./snippets/info-scroll-wrapper/info-scroll-wrapper.component";

registerLocaleData(en);

@NgModule({
  declarations: [
    AppComponent,
    OverviewComponent,
    HeaderComponent,
    FooterComponent,
    ProcessingApplicationsComponent,
    FinishedApplicationsComponent,
    ApplicationCardComponent,
    ScrollWrapperComponent,
    ApplicationInformationComponent,
    ProcessingApplicationListElementComponent,
    FinishedApplicationListElementComponent,
    ApplicationBoardComponent,
    InfoScrollWrapperComponent
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
    NzProgressModule
  ],
  providers: [
    { provide: NZ_I18N, useValue: en_US }
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
