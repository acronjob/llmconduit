/** Route → view-component map. Kept out of the .tsx so react-refresh stays happy. */
import type { ComponentType } from 'react';
import type { RouteName } from '../router/useHashRoute';
import { FlowsView } from './FlowsView';
import { TopologyView } from './TopologyView';
import { SankeyView } from './SankeyView';
import { TheaterView } from './TheaterView';
import { OverviewView } from './overview/OverviewView';
import { SessionsView } from './sessions/SessionsView';

export const VIEW_BY_ROUTE: Record<RouteName, ComponentType> = {
  flows: FlowsView,
  sessions: SessionsView,
  topology: TopologyView,
  sankey: SankeyView,
  theater: TheaterView,
  overview: OverviewView,
};
