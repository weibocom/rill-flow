export default {
  definitions: {
    record: "流程定义",
    list: "流程列表",
    detail: "流程详情",
  },
  instances: {
    record: "执行记录",
    detail: "执行详情",
    record_detail: "记录详情",
    opt: "操作",
    option: {
      detail: "详情"
    },
    status: {
      SUCCEEDED: "成功",
      FAILED: "失败",
      READY: "就绪",
      NOT_STARTED: "未开始",
      RUNNING: "进行中",
      SKIPPED: "跳过",

    },
    columns: {
      submit_time: "创建时间",
      execution_id: "执行ID",
      business_id: "业务名称",
      feature_id: "服务名称",
      status: "状态"
    },
    form_props: {
      business_id: "业务名称",
      business_placeholder: "请输入业务名称",
      feature_id: "服务名称",
      feature_placeholder: "业务名称与服务名称联动",
      execution_id: "执行ID",
      time_range_select: "日期时间范围",
      start_time_placeholder: "开始日期、时间",
      end_time_placeholder: "结束日期、时间",
    },
    graph: {
      execution_detail_expire_message: "执行详情已过期",
      execution_detail_none_message: "请先到执行列表中选择要查看的记录",
      grid: {
        title: "Dag执行详情",
        schema: {
          execution_id: "执行ID",
          status: "状态",
          progress: "进度",
          start_time: "开始时间",
          end_time: "结束时间",
          context: "上下文信息",
          context_key: "参数名",
          context_value: "参数值",
          error_result_msg: "执行异常信息",
          trace: "Trace日志信息",
          trace_detail: "详情",
        }
      },
      node: {
        title: "节点详情",
        schema: {
          name: "节点名称",
          category: "节点类型",
          resource_protocol: "资源类型",
          resource_name: "资源地址",
          pattern: "同步执行",
          tolerance: "失败是否跳过",
          start_time: "开始时间",
          end_time: "结束时间",
          status: "状态",
          input_msg: "节点输入信息",
          input_mappings: "输入映射信息",
          input_mappings_source: "参数来源",
          input_mappings_target: "参数目标位置",
          output_mappings_source: "参数来源",
          output_mappings_target: "参数目标位置",
          conditions: "条件信息",
          conditions_key: "条件列表",
          interruptions: "中断条件",
          interruptions_key: "中断条件列表",
          timeline: "时间线",
          timeline_key: "参数名",
          timeline_value: "参数值",
          choices: "选择器",
          choices_key: "选择条件",
          choices_value: "选择节点",
          output_mappings: "输出映射信息",
          input_msg_key: "参数名",
          input_msg_value: "参数值",
          iteration_msg: "迭代器信息",
          iteration_msg_key: "参数名",
          iteration_msg_value: "参数值",
          output_msg: "节点输出信息",
          output_msg_key: "参数名",
          output_msg_value: "参数值",
          error_result_msg: "执行异常信息",
        }
      }
    }
  },
  trigger: {
    index: "触发器",
    list: "触发器列表",
  }
}
