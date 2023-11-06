import {Component, OnInit} from '@angular/core';
import {OverviewService} from "./overview.service";
import {FormBuilder, FormGroup} from "@angular/forms";
import {Job} from "../../model/Job";

@Component({
  selector: 'app-home',
  templateUrl: './overview.component.html',
  styleUrls: ['./overview.component.less']
})
export class OverviewComponent implements OnInit {
  filterForm: FormGroup;
  jobs: Job[] = [];
  filteredJobs: Job[] = [];
  tableIsLoading = false;   // Whether the table is loading
  currentPageIndex = 1;     // Current page index
  pageSize = 10;            // Number of jobs per page
  currentPageJob: Job[] = [];

  constructor(private overviewService: OverviewService, private formBuilder: FormBuilder) {}

  ngOnInit() {
    this.tableIsLoading = true;

    this.overviewService.getAllJobs().subscribe(res => {
      this.jobs = res;
      this.filteredJobs = this.jobs;
      this.filterJob();
    });

    this.filterForm = this.formBuilder.group({
      name: [null, null],
      status: [null, null]
    })
  }

  /**
   * Change the page index
   * @param index The new page index
   */
  onPageIndexChange(index: number) {
    this.currentPageIndex = index;
    this.filterJob();
  }

  /**
   * Change the page size
   * @param size The new page size
   */
  onPageSizeChange(size: number) {
    this.pageSize = size;
    this.filterJob()
  }

  onFilterJob() {
    this.tableIsLoading = true;
    this.currentPageIndex = 1;
    this.filterJob();
  }

  /**
   * Filter jobs based on name and status
   */
  filterJob() {
    const filterName = this.filterForm.value.name;
    const filterStatus = this.filterForm.value.status;
    this.filteredJobs = this.jobs;

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

    // Update the table
    const startIndex = (this.currentPageIndex-1) * this.pageSize;
    const endIndex = startIndex + this.pageSize;
    this.currentPageJob = this.filteredJobs.slice(startIndex, endIndex);
    this.tableIsLoading = false;
  }
}
