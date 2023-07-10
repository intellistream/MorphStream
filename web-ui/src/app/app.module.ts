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
import {OverviewComponent} from "./pages/overview/overview.component";
import {HeaderComponent} from "./common/layout/header/header.component";
import {FooterComponent} from "./common/layout/footer/footer.component";
import {PocessingApplicationsComponent} from "./pages/applications/pocessing-applications/pocessing-applications.component";
import {FinishedApplicationsComponent} from "./pages/applications/finished-applications/finished-applications.component";
import {ApplicationCardComponent} from "./snippets/application-card/application-card.component";
import {NzGridModule} from "ng-zorro-antd/grid";
import {ScrollWrapperComponent} from "./snippets/scroll-wrapper/scroll-wrapper.component";

registerLocaleData(en);

@NgModule({
  declarations: [
    AppComponent,
    OverviewComponent,
    HeaderComponent,
    FooterComponent,
    PocessingApplicationsComponent,
    FinishedApplicationsComponent,
    ApplicationCardComponent,
    ScrollWrapperComponent
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
    NzGridModule
  ],
  providers: [
    { provide: NZ_I18N, useValue: en_US }
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
