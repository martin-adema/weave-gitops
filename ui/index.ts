import AutomationsTable from "./components/AutomationsTable";
import BucketDetail from "./components/BucketDetail";
import Button from "./components/Button";
import DagGraph from "./components/DagGraph";
import DataTable, {
  filterByStatusCallback,
  filterConfig,
} from "./components/DataTable";
import DependenciesView from "./components/DependenciesView";
import EventsTable from "./components/EventsTable";
import Flex from "./components/Flex";
import FluxRuntime from "./components/FluxRuntime";
import Footer from "./components/Footer";
import GithubDeviceAuthModal from "./components/GithubDeviceAuthModal";
import GitRepositoryDetail from "./components/GitRepositoryDetail";
import HelmChartDetail from "./components/HelmChartDetail";
import HelmReleaseDetail from "./components/HelmReleaseDetail";
import HelmRepositoryDetail from "./components/HelmRepositoryDetail";
import Icon, { IconType } from "./components/Icon";
import InfoList from "./components/InfoList";
import Interval from "./components/Interval";
import KubeStatusIndicator from "./components/KubeStatusIndicator";
import KustomizationDetail from "./components/KustomizationDetail";
import Link from "./components/Link";
import LoadingPage from "./components/LoadingPage";
import MessageBox from "./components/MessageBox";
import Metadata from "./components/Metadata";
import NotificationsTable from "./components/NotificationsTable";
import OCIRepositoryDetail from "./components/OCIRepositoryDetail";
import Page from "./components/Page";
import ProviderDetail from "./components/ProviderDetail";
import ReconciledObjectsTable from "./components/ReconciledObjectsTable";
import ReconciliationGraph from "./components/ReconciliationGraph";
import RepoInputWithAuth from "./components/RepoInputWithAuth";
import SourceLink from "./components/SourceLink";
import SourcesTable from "./components/SourcesTable";
import SubRouterTabs, { RouterTab } from "./components/SubRouterTabs";
import Timestamp from "./components/Timestamp";
import UserSettings from "./components/UserSettings";
import YamlView from "./components/YamlView";
import AppContextProvider from "./contexts/AppContext";
import AuthContextProvider, { Auth, AuthCheck } from "./contexts/AuthContext";
import CallbackStateContextProvider from "./contexts/CallbackStateContext";
import CoreClientContextProvider, {
  UnAuthorizedInterceptor,
} from "./contexts/CoreClientContext";
import { LinkResolverProvider } from "./contexts/LinkResolverContext";
import { useListAutomations } from "./hooks/automations";
import { useFeatureFlags } from "./hooks/featureflags";
import { useListFluxCrds, useListFluxRuntimeObjects } from "./hooks/flux";
import { useIsAuthenticated } from "./hooks/gitprovider";
import { useListAlerts, useListProviders } from "./hooks/notifications";
import { useGetObject, useListObjects } from "./hooks/objects";
import { useListSources } from "./hooks/sources";
import { Applications as applicationsClient } from "./lib/api/applications/applications.pb";
import { Core as coreClient } from "./lib/api/core/core.pb";
import { Kind } from "./lib/api/core/types.pb";
import { formatURL } from "./lib/nav";
import {
  Automation,
  Bucket,
  GitRepository,
  HelmChart,
  HelmRepository,
  OCIRepository,
} from "./lib/objects";
import {
  clearCallbackState,
  getCallbackState,
  getProviderToken,
} from "./lib/storage";
import { muiTheme, theme } from "./lib/theme";
import { V2Routes } from "./lib/types";
import { isAllowedLink, statusSortHelper } from "./lib/utils";
import OAuthCallback from "./pages/OAuthCallback";
import SignIn from "./pages/SignIn";

export {
  AppContextProvider,
  applicationsClient,
  Auth,
  AuthCheck,
  AuthContextProvider,
  Automation,
  AutomationsTable,
  Bucket,
  BucketDetail,
  Button,
  CallbackStateContextProvider,
  clearCallbackState,
  coreClient,
  CoreClientContextProvider,
  DataTable,
  DagGraph,
  DependenciesView,
  EventsTable,
  Flex,
  filterByStatusCallback,
  filterConfig,
  FluxRuntime,
  Footer,
  formatURL,
  getCallbackState,
  getProviderToken,
  GithubDeviceAuthModal,
  GitRepository,
  GitRepositoryDetail,
  HelmChart,
  HelmRepository,
  HelmChartDetail,
  HelmReleaseDetail,
  HelmRepositoryDetail,
  Icon,
  IconType,
  InfoList,
  Interval,
  isAllowedLink,
  Kind,
  KubeStatusIndicator,
  KustomizationDetail,
  Link,
  LinkResolverProvider,
  LoadingPage,
  MessageBox,
  Metadata,
  muiTheme,
  NotificationsTable,
  OAuthCallback,
  OCIRepository,
  OCIRepositoryDetail,
  Page,
  ProviderDetail,
  ReconciledObjectsTable,
  ReconciliationGraph,
  RepoInputWithAuth,
  RouterTab,
  SignIn,
  SourceLink,
  SourcesTable,
  statusSortHelper,
  SubRouterTabs,
  theme,
  Timestamp,
  UnAuthorizedInterceptor,
  useFeatureFlags,
  useGetObject,
  useListObjects,
  useIsAuthenticated,
  useListAlerts,
  useListAutomations,
  useListFluxCrds,
  useListFluxRuntimeObjects,
  useListProviders,
  useListSources,
  UserSettings,
  V2Routes,
  YamlView,
};
