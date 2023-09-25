import {AfterViewInit, Component, ElementRef, Input, ViewChild} from '@angular/core';

import G6 from '@antv/g6';

@Component({
  selector: 'app-tpg-graph',
  templateUrl: './tpg-graph.component.html',
  styleUrls: ['./tpg-graph.component.less']
})
export class TpgGraphComponent implements AfterViewInit {
  // test data
  @Input()
  data = {
    // Nodes
    nodes: [
      {
        id: '1',
        label: '1',
        layer: '1'
      },
      {
        id: '2',
        label: '2',
        layer: '2'
      },
      {
        id: '3',
        label: '3',
        layer: '3'
      },
      {
        id: '4',
        label: '4',
        layer: '2'
      },
      {
        id: '5',
        label: '5',
        layer: '3'
      },
      {
        id: '6',
        label: '6',
        layer: '4'
      },
      {
        id: '7',
        label: '7',
        layer: '1'
      },
      {
        id: '8',
        label: '8',
        layer: '2'
      }
    ],
    // Edges
    edges: [
      {
        source: '1',
        target: '2',
        style: {
          stroke: '#B8534F'
        }
      },
      {
        source: '2',
        target: '4',
        style: {
          stroke: '#81B366',
          lineDash: [2, 2],
          endArrow: false
        }
      },
      {
        source: '3',
        target: '5',
        style: {
          stroke: '#81B366',
          lineDash: [2, 2],
          endArrow: false,
        }
      },
      {
        source: '2',
        target: '3',
        style: {
          stroke: '#B8534F'
        }
      },
      {
        source: '1',
        target: '4',
        style: {
          stroke: '#D5B556'
        }
      },
      {
        source: '4',
        target: '3',
        style: {
          stroke: '#D5B556'
        }
      },
      {
        source: '4',
        target: '5',
        style: {
          stroke: '#B8534F'
        }
      },
      {
        source: '5',
        target: '6',
        style: {
          stroke: '#B8534F'
        }
      },
      {
        source: '7',
        target: '8',
        style: {
          stroke: '#B8534F'
        }
      }
    ],
  }

  @ViewChild('graphContainer', { static: false }) graphContainer!: ElementRef;

  ngAfterViewInit() {
    let containerWidth = 0;
    if (this.graphContainer) {
      const container: HTMLDivElement = this.graphContainer.nativeElement;
      containerWidth = container.offsetWidth;
    }



    const grid = new G6.Grid();

    // // minimap plugin
    // const minimap = new G6.Minimap({
    //   size: [140, 140],
    //   className: 'minimap',
    //   type: 'delegate',
    //   container: 'graph-container'
    // });

    // tooltip plugin
    const tooltip = new G6.Tooltip({
      offsetX: 10,
      offsetY: 20,
      getContent(e) {
        const outDiv = document.createElement('div');
        outDiv.style.width = '180px';
        outDiv.innerHTML = `
            <h4>${e!.item!.getModel().label}</h4>
            <ul>
                <li>Label: ${e!.item!.getModel().label}</li>
            </ul>`
        return outDiv
      },
      itemTypes: ['node']
    });

    const graph = new G6.Graph({
      modes: {
        default: ['drag-canvas', 'zoom-canvas'],  // 'drag-node'
      },
      container: 'graph-container',
      width: containerWidth,
      height: 500,
      layout: {
        type: 'concentric',
        nodeSize: 30,
        preventOverlap: true,
        sortBy: 'layer',
      },
      defaultEdge: {
        style: {
          endArrow: true,
          lineWidth: 2,
        }
      },
      plugins: [tooltip, grid]
    });
    graph.data(this.data);
    graph.render();

    const gridContainer = this.elementRef.nativeElement.querySelector(".g6-grid-container");
    gridContainer.style.zIndex = 1;

    const canvas = this.elementRef.nativeElement.querySelector("canvas");
    canvas.style.position = "relative";
    canvas.style.zIndex = 2;
  }

  constructor(private elementRef: ElementRef) {
  }
}
