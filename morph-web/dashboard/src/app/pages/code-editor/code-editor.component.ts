import {AfterViewInit, Component, ViewChild} from '@angular/core';

import 'codemirror/mode/clike/clike';
import {FormControl, FormGroup, NonNullableFormBuilder, Validators} from "@angular/forms";

@Component({
  selector: 'app-code-editor',
  templateUrl: './code-editor.component.html',
  styleUrls: ['./code-editor.component.less']
})
export class CodeEditorComponent implements AfterViewInit{
  job = '';
  parallelism = 4;
  // @ViewChild('codeEditor') editor: any;
  // codemirrorOptions = {
  //   mode: "text/x-java",
  //   // mode: "markdown",
  //   indentWithTabs: true,
  //   smartIndent: true,
  //   lineNumbers: true,
  //   lineWrapping: true,
  //   matchBrackets: true,
  //   autofocus: true,
  //   theme: "material",
  // }

  code = `import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor'`;
  isSubmittingNewJob = false;

  submitForm: FormGroup<{
    job: FormControl<string>;
    parallelism: FormControl<number>;
    startNow: FormControl<boolean>;
  }>;

  constructor(private fb: NonNullableFormBuilder) {
    this.submitForm = this.fb.group({
      job: ['', [Validators.required]],
      parallelism: [4, [Validators.required]],
      startNow: [false, [Validators.required]]
    })
  }

  ngAfterViewInit(): void {
  }

  submitNewJob() {
    this.isSubmittingNewJob = true;
  }

  onCancelSubmit() {
    this.isSubmittingNewJob = false;
  }

  confirmSubmit() {
    if (this.submitForm.valid) {

    } else {
      Object.values(this.submitForm.controls).forEach(control => {
        if (control.invalid) {
          control.markAsDirty();
          control.updateValueAndValidity({onlySelf: true});
        }
      });
    }
  }
}
