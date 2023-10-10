import {Component, ElementRef, OnInit, ViewChild} from '@angular/core';
import {OverviewService} from "./overview.service";
import {BasicApplication} from "../../model/BasicApplication";
import {FormBuilder, FormGroup} from "@angular/forms";

@Component({
  selector: 'app-home',
  templateUrl: './overview.component.html',
  styleUrls: ['./overview.component.less']
})
export class OverviewComponent implements OnInit {
  @ViewChild('runningAppContainer')
  scrollContainer!: ElementRef<HTMLElement>;

  filterForm: FormGroup;

  constructor(private overviewService: OverviewService, private formBuilder: FormBuilder) {}

  completedApplications: BasicApplication[] = [];

  filteredJobs: BasicApplication[] = [];

  ngOnInit() {
    this.tableIsLoading = true;

    this.overviewService.getAllHistoricalJobs().subscribe(res => {
      this.completedApplications = res;
      this.filteredJobs = this.completedApplications;
      this.filterJob();
    });

    this.filterForm = this.formBuilder.group({
      name: [null, null],
      status: [null, null]
    })
  }

  tableIsLoading = false;
  currentPageIndex = 1;
  pageSize = 10;
  currentPageJob: BasicApplication[] = [];

  onPageIndexChange(index: number) {
    this.currentPageIndex = index;
    this.filterJob();
  }

  onPageSizeChange(size: number) {
    this.pageSize = size;
    this.filterJob()
  }


  onFilterJob() {
    this.tableIsLoading = true;
    this.currentPageIndex = 1;
    this.filterJob();
  }

  filterJob() {
    const filterName = this.filterForm.value.name;
    const filterStatus = this.filterForm.value.status;
    this.filteredJobs = this.completedApplications;

    if (filterName != null) {
      this.filteredJobs = this.filteredJobs.filter(job =>
        job.name.includes(`${filterName}`)
      );
    }

    if (filterStatus != null) {
      this.filteredJobs = this.filteredJobs.filter(job =>
        job.isRunning == filterStatus
      );
    }

    const startIndex = (this.currentPageIndex-1) * this.pageSize;
    const endIndex = startIndex + this.pageSize;
    this.currentPageJob = this.filteredJobs.slice(startIndex, endIndex);
    this.tableIsLoading = false;
  }
}
