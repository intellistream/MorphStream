import {AfterViewInit, Component, ViewChild} from '@angular/core';

import 'codemirror/mode/clike/clike';
// import 'codemirror/mode/markdown/markdown';

@Component({
  selector: 'app-code-editor',
  templateUrl: './code-editor.component.html',
  styleUrls: ['./code-editor.component.less']
})
export class CodeEditorComponent implements AfterViewInit{
  code: string = "# Your initial code here\n"
    + "class Hello {\n"
    + "\tprivate int i;\n"
    + "}\n";

  @ViewChild('codeEditor') editor: any;
  codemirrorOptions = {
    mode: "text/x-java",
    // mode: "markdown",
    indentWithTabs: true,
    smartIndent: true,
    lineNumbers: true,
    lineWrapping: true,
    matchBrackets: true,
    autofocus: true,
    theme: "material",
  }

  constructor() {}

  ngAfterViewInit(): void {
  }
}
