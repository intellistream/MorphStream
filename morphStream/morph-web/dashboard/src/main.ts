import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';

// import 'codemirror/mode/clike/clike';
import 'codemirror/mode/javascript/javascript';


import { AppModule } from './app/app.module';

platformBrowserDynamic().bootstrapModule(AppModule)
  .catch(err => console.error(err));
