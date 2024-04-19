import { defineStore } from 'pinia';
import { store } from '/@/store';
import { FlowParams } from '/@/modules/flowParams';

interface FlowGraphState {
  params: FlowParams;
}

export const useFlowGraphStore = defineStore({
  id: 'flow-graph-params',
  state: (): FlowGraphState => ({
    params: null,
  }),
  getters: {
    getFlowGraphParams(state): FlowParams {
      return state.params;
    },
  },
  actions: {
    setFlowGraphParams(params: FlowParams) {
      this.params = params;
    },
    resetFlowGraphParams() {
      this.params = null;
    },
  },
});

export function useFlowGraphStoreWithOut() {
  return useFlowGraphStore(store);
}
