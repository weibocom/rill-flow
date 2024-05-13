import { defineStore } from 'pinia';
import { store } from '../index';
import { NodePrototypeRegistry } from '../../models/nodeTemplate';
import { FlowParams } from '../../models/flowParams';
import { ref, toRaw } from "vue";
import {FlowGraph} from "../../models/flowGraph";
import {X6FlowGraph} from "../../models/X6FlowGraph";

export const useFlowParamsStore = defineStore('flow-params', () => {
  const flowParams = ref<FlowParams>({} as FlowParams);
  const flowGraph = ref<FlowGraph>(new X6FlowGraph());
  const nodePrototypeRegistry = ref<NodePrototypeRegistry>(new NodePrototypeRegistry());

  function getFlowParams(): FlowParams {
    const flowParamsObj = toRaw(flowParams.value);
    if (flowParamsObj?.id === undefined) {
      if (localStorage.getItem('flowParams') !== null) {
        const localFlowParams: FlowParams = JSON.parse(localStorage.getItem('flowParams'));
        this.setFlowParams(localFlowParams);
        return flowParams.value;
      }
      const defaultFlowParams = new FlowParams('', 'create', '', '', '', '', '', '', [])
      this.setFlowParams(defaultFlowParams);

    }
    return flowParams.value;
  }
  function setFlowParams(params: FlowParams) {
    flowParams.value = params;
    localStorage.setItem('flowParams', JSON.stringify(params));
  }

  function setLanguage(language: string) {
    localStorage.setItem('language', language);
  }

  function getLanguage() {
    return localStorage.getItem("language");
  }

  function getFlowGraph(): FlowGraph {
    return flowGraph.value;
  }

  function getNodePrototypeRegistry() {
    return nodePrototypeRegistry.value;
  }

  return {
    flowParams,
    getFlowParams,
    getFlowGraph,
    setFlowParams,
    getNodePrototypeRegistry,
    setLanguage,
    getLanguage,
  };
});

export function useFlowStoreWithOut() {
  return useFlowParamsStore(store);
}
