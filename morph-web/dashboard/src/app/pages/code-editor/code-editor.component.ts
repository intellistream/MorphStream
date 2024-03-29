import {AfterViewInit, Component} from '@angular/core';

import 'codemirror/mode/clike/clike';
import {FormControl, FormGroup, NonNullableFormBuilder, Validators} from "@angular/forms";
import {CodeEditorService} from "./code-editor.service";
import {NzMessageService} from "ng-zorro-antd/message";

@Component({
  selector: 'app-code-editor',
  templateUrl: './code-editor.component.html',
  styleUrls: ['./code-editor.component.less']
})
export class CodeEditorComponent implements AfterViewInit{
  job = '';
  parallelism = 4;

  code = `import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor'`;
  isSubmittingNewJob = false;

  submitForm: FormGroup<{
    job: FormControl<string>;
    parallelism: FormControl<number>;
    startNow: FormControl<boolean>;
  }>;

  constructor(private fb: NonNullableFormBuilder, private codeEditorService: CodeEditorService, private message: NzMessageService) {
    this.submitForm = this.fb.group({
      job: ['', [Validators.required]],
      parallelism: [4, [Validators.required]],
      startNow: [false, [Validators.required]]
    });
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
      this.codeEditorService.submitNewJob(this.submitForm.value.job!, this.submitForm.value.parallelism!, this.submitForm.value.startNow!, this.code).subscribe(res => {
        this.message.success(`Job ${this.submitForm.value.job} is submitted successfully`);
        this.isSubmittingNewJob = false;
      });
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
