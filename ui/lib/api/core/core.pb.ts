/* eslint-disable */
// @ts-nocheck
/*
* This file is a generated Typescript file for GRPC Gateway, DO NOT MODIFY
*/

import * as fm from "../../fetch.pb"
import * as Gitops_coreV1Types from "./types.pb"
export type Pagination = {
  pageSize?: number
  pageToken?: string
}

export type ListError = {
  clusterName?: string
  namespace?: string
  message?: string
}

export type ListFluxRuntimeObjectsRequest = {
  namespace?: string
  clusterName?: string
}

export type ListFluxRuntimeObjectsResponse = {
  deployments?: Gitops_coreV1Types.Deployment[]
  errors?: ListError[]
}

export type ListFluxCrdsRequest = {
  clusterName?: string
}

export type ListFluxCrdsResponse = {
  crds?: Gitops_coreV1Types.Crd[]
  errors?: ListError[]
}

export type GetObjectRequest = {
  name?: string
  namespace?: string
  kind?: string
  clusterName?: string
}

export type GetObjectResponse = {
  object?: Gitops_coreV1Types.Object
}

export type ListObjectsRequest = {
  namespace?: string
  kind?: string
  clusterName?: string
  labels?: {[key: string]: string}
}

export type ListObjectsResponse = {
  objects?: Gitops_coreV1Types.Object[]
  errors?: ListError[]
}

export type GetReconciledObjectsRequest = {
  automationName?: string
  namespace?: string
  automationKind?: string
  kinds?: Gitops_coreV1Types.GroupVersionKind[]
  clusterName?: string
}

export type GetReconciledObjectsResponse = {
  objects?: Gitops_coreV1Types.Object[]
}

export type GetChildObjectsRequest = {
  groupVersionKind?: Gitops_coreV1Types.GroupVersionKind
  namespace?: string
  parentUid?: string
  clusterName?: string
}

export type GetChildObjectsResponse = {
  objects?: Gitops_coreV1Types.Object[]
}

export type GetFluxNamespaceRequest = {
}

export type GetFluxNamespaceResponse = {
  name?: string
}

export type ListNamespacesRequest = {
}

export type ListNamespacesResponse = {
  namespaces?: Gitops_coreV1Types.Namespace[]
}

export type ListEventsRequest = {
  involvedObject?: Gitops_coreV1Types.ObjectRef
}

export type ListEventsResponse = {
  events?: Gitops_coreV1Types.Event[]
}

export type SyncFluxObjectRequest = {
  objects?: Gitops_coreV1Types.ObjectRef[]
  withSource?: boolean
}

export type SyncFluxObjectResponse = {
}

export type GetVersionRequest = {
}

export type GetVersionResponse = {
  semver?: string
  commit?: string
  branch?: string
  buildTime?: string
  fluxVersion?: string
  kubeVersion?: string
}

export type GetFeatureFlagsRequest = {
}

export type GetFeatureFlagsResponse = {
  flags?: {[key: string]: string}
}

export type ToggleSuspendResourceRequest = {
  objects?: Gitops_coreV1Types.ObjectRef[]
  suspend?: boolean
}

export type ToggleSuspendResourceResponse = {
}

export type GetSessionLogsRequest = {
  sessionId?: string
  token?: string
  clusterName?: string
  namespace?: string
}

export type GetSessionLogsResponse = {
  logs?: string[]
  nextToken?: string
}

export class Core {
  static GetObject(req: GetObjectRequest, initReq?: fm.InitReq): Promise<GetObjectResponse> {
    return fm.fetchReq<GetObjectRequest, GetObjectResponse>(`/v1/object/${req["name"]}?${fm.renderURLSearchParams(req, ["name"])}`, {...initReq, method: "GET"})
  }
  static ListObjects(req: ListObjectsRequest, initReq?: fm.InitReq): Promise<ListObjectsResponse> {
    return fm.fetchReq<ListObjectsRequest, ListObjectsResponse>(`/v1/objects`, {...initReq, method: "POST", body: JSON.stringify(req)})
  }
  static ListFluxRuntimeObjects(req: ListFluxRuntimeObjectsRequest, initReq?: fm.InitReq): Promise<ListFluxRuntimeObjectsResponse> {
    return fm.fetchReq<ListFluxRuntimeObjectsRequest, ListFluxRuntimeObjectsResponse>(`/v1/flux_runtime_objects?${fm.renderURLSearchParams(req, [])}`, {...initReq, method: "GET"})
  }
  static ListFluxCrds(req: ListFluxCrdsRequest, initReq?: fm.InitReq): Promise<ListFluxCrdsResponse> {
    return fm.fetchReq<ListFluxCrdsRequest, ListFluxCrdsResponse>(`/v1/flux_crds?${fm.renderURLSearchParams(req, [])}`, {...initReq, method: "GET"})
  }
  static GetReconciledObjects(req: GetReconciledObjectsRequest, initReq?: fm.InitReq): Promise<GetReconciledObjectsResponse> {
    return fm.fetchReq<GetReconciledObjectsRequest, GetReconciledObjectsResponse>(`/v1/reconciled_objects`, {...initReq, method: "POST", body: JSON.stringify(req)})
  }
  static GetChildObjects(req: GetChildObjectsRequest, initReq?: fm.InitReq): Promise<GetChildObjectsResponse> {
    return fm.fetchReq<GetChildObjectsRequest, GetChildObjectsResponse>(`/v1/child_objects`, {...initReq, method: "POST", body: JSON.stringify(req)})
  }
  static GetFluxNamespace(req: GetFluxNamespaceRequest, initReq?: fm.InitReq): Promise<GetFluxNamespaceResponse> {
    return fm.fetchReq<GetFluxNamespaceRequest, GetFluxNamespaceResponse>(`/v1/namespace/flux`, {...initReq, method: "POST", body: JSON.stringify(req)})
  }
  static ListNamespaces(req: ListNamespacesRequest, initReq?: fm.InitReq): Promise<ListNamespacesResponse> {
    return fm.fetchReq<ListNamespacesRequest, ListNamespacesResponse>(`/v1/namespaces?${fm.renderURLSearchParams(req, [])}`, {...initReq, method: "GET"})
  }
  static ListEvents(req: ListEventsRequest, initReq?: fm.InitReq): Promise<ListEventsResponse> {
    return fm.fetchReq<ListEventsRequest, ListEventsResponse>(`/v1/events?${fm.renderURLSearchParams(req, [])}`, {...initReq, method: "GET"})
  }
  static SyncFluxObject(req: SyncFluxObjectRequest, initReq?: fm.InitReq): Promise<SyncFluxObjectResponse> {
    return fm.fetchReq<SyncFluxObjectRequest, SyncFluxObjectResponse>(`/v1/sync`, {...initReq, method: "POST", body: JSON.stringify(req)})
  }
  static GetVersion(req: GetVersionRequest, initReq?: fm.InitReq): Promise<GetVersionResponse> {
    return fm.fetchReq<GetVersionRequest, GetVersionResponse>(`/v1/version?${fm.renderURLSearchParams(req, [])}`, {...initReq, method: "GET"})
  }
  static GetFeatureFlags(req: GetFeatureFlagsRequest, initReq?: fm.InitReq): Promise<GetFeatureFlagsResponse> {
    return fm.fetchReq<GetFeatureFlagsRequest, GetFeatureFlagsResponse>(`/v1/featureflags?${fm.renderURLSearchParams(req, [])}`, {...initReq, method: "GET"})
  }
  static ToggleSuspendResource(req: ToggleSuspendResourceRequest, initReq?: fm.InitReq): Promise<ToggleSuspendResourceResponse> {
    return fm.fetchReq<ToggleSuspendResourceRequest, ToggleSuspendResourceResponse>(`/v1/suspend`, {...initReq, method: "POST", body: JSON.stringify(req)})
  }
  static GetSessionLogs(req: GetSessionLogsRequest, initReq?: fm.InitReq): Promise<GetSessionLogsResponse> {
    return fm.fetchReq<GetSessionLogsRequest, GetSessionLogsResponse>(`/v1/session/${req["sessionId"]}/logs?${fm.renderURLSearchParams(req, ["sessionId"])}`, {...initReq, method: "GET"})
  }
}