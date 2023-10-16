import {TPGEdge} from "./TPGEdge";

export interface TPGNode {
  operationID: string
  txnType: string
  edges: TPGEdge[]
}
