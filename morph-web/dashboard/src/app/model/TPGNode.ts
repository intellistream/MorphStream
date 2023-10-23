import {TPGEdge} from "./TPGEdge";

export interface TPGNode {
  operationID: string
  txnType: string
  targetTable: string
  targetKey: string
  edges: TPGEdge[]
}
