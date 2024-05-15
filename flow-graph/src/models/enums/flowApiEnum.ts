export enum FlowApiEnum {
  QUERY_DAG = '/flow/bg/get_descriptor.json',
  SUBMIT_DAG = '/flow/bg/manage/descriptor/add_descriptor.json',
  QUERY_EXECUTION = '/flow/bg/get_execution.json',
  SUBMIT_EXECUTION = '/flow/submit.json',
  QUERY_META_NODES = '/flow/template/get_meta_data_list.json',
  QUERY_NODE_PROTOTYPES = '/flow/template/get_template_prototypes.json?enable=1',
}
