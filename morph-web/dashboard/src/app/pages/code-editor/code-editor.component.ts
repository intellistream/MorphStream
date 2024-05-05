import {AfterViewInit, Component} from '@angular/core';

import 'codemirror/mode/clike/clike';
import {FormControl, FormGroup, NonNullableFormBuilder, Validators} from "@angular/forms";
import {CodeEditorService} from "./code-editor.service";
import {NzMessageService} from "ng-zorro-antd/message";
import {NzUploadFile} from "ng-zorro-antd/upload";
import {Subscription} from "rxjs";

@Component({
  selector: 'app-code-editor',
  templateUrl: './code-editor.component.html',
  styleUrls: ['./code-editor.component.less']
})
export class CodeEditorComponent implements AfterViewInit {
  job = '';
  parallelism = 4;
  code = 'public class Job extends Clients {\n' +
    '    @Override\n' +
    '    public boolean transactionUDF(StateAccess access) {\n' +
    '        boolean isSuccess = true;\n' +
    '        // Your code here\n' +
    '        return isSuccess;\n' +
    '    }\n' +
    '\n' +
    '    @Override\n' +
    '    public Result postUDF(String txnName, HashMap<String, StateAccess> stateAccessMap) {\n' +
    '        Result result = new Result();\n' +
    '        // Your code here\n' +
    '        return result;\n' +
    '    }\n' +
    '}';

  description =
    '{\n' +
    '    "name": "Job",\n' +
    '    "operatorDescription": [\n' +
    '        {\n' +
    '            "name": "operatorName",\n' +
    '            "transactionDescription": [\n' +
    '                {\n' +
    '                    "name": "transactionName",\n' +
    '                    "stateAccessDescription": [\n' +
    '                        {\n' +
    '                            "name": "stateName",\n' +
    '                            "accessType": "read / write",\n' +
    '                            "stateObjectDescription": [\n' +
    '                                {\n' +
    '                                    "name": "stateObjectName",\n' +
    '                                    "accessType": "read / write",\n' +
    '                                    "tableName": "tableName",\n' +
    '                                    "keyName": "keyName",\n' +
    '                                    "valueName": "objectValueName",\n' +
    '                                    "keyIndex": 0\n' +
    '                                }\n' +
    '                            ],\n' +
    '                            "valueName": "valueName"\n' +
    '                        }\n' +
    '                    ]\n' +
    '                }\n' +
    '            ]\n' +
    '        }\n' +
    '    ]\n' +
    '}\n';

  isSubmittingNewJob = false;
  isSubmittingFiles = false;
  submitForm: FormGroup<{
    job: FormControl<string>;
    parallelism: FormControl<number>;
    startNow: FormControl<boolean>;
    // configFile: FormControl;
  }>;
  fileList: NzUploadFile[] = [];

  ngAfterViewInit(): void {
  }

  constructor(private fb: NonNullableFormBuilder, private codeEditorService: CodeEditorService, private message: NzMessageService) {
    this.submitForm = this.fb.group({
      job: ['', [Validators.required]],
      parallelism: [4, [Validators.required]],
      startNow: [false, [Validators.required]],
    });
  }

  submitNewJob() {
    const description = JSON.parse(this.description);
    if (description.name !== undefined && description.name !== "") {
      this.job = description.name;
    }
    this.isSubmittingNewJob = true;
  }

  onCancelSubmit() {
    this.isSubmittingNewJob = false;
  }

  onConfirmSubmit() {
    if (this.submitForm.valid) {
      // this.codeEditorService.submitNewJobByConfigFile(this.submitForm.value.job!, this.submitForm.value.parallelism!, this.submitForm.value.startNow!, this.code, this.fileList[0]).subscribe(res => {
      //   this.message.success(`Job ${this.submitForm.value.job} is submitted successfully`);
      //   this.isSubmittingNewJob = false;
      // });
      this.codeEditorService.submitNewJobByDescription(this.submitForm.value.job!, this.submitForm.value.parallelism!, this.submitForm.value.startNow!, this.code, this.description).subscribe(res => {
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
      if (this.submitForm.controls['configFile'].invalid) {
        this.message.error('Please upload a description file');
      }
    }
  }

  onSubmitFiles() {
    this.isSubmittingFiles = true;
  }

  onCancelSubmitFiles() {
    this.isSubmittingFiles = false;
    this.fileList = [];
  }

  onConfirmSubmitFiles() {
    this.isSubmittingFiles = false;
    this.codeEditorService.onConfirmSubmitFiles(this.fileList, this.parallelism, false).subscribe(res => {
      if (res) {
        this.message.success('Files are submitted successfully');
      } else {
        this.message.error('Failed to submit files');
      }
      this.fileList = [];
    });
  }

  dummyRequestHandler = (item: any): Subscription => {
    setTimeout(() => {
      item.onSuccess(null, item.file, null); // simulating a success callback
    }, 0);
    return new Subscription(); // a dummy subscription
  };

  handleFileChange(event: any): void {
    // Check the latest fileList status
    if (event.fileList.length > 0) {
      const latestFile = event.fileList[event.fileList.length - 1].originFileObj;
      this.submitForm.controls['configFile'].setValue(latestFile);
    } else {
      // clear the form control
      this.submitForm.controls['configFile'].reset();
    }
  }
}
