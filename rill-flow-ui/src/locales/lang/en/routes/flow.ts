export default {
  definitions: {
    record: "Flow Definition",
    list: "Flow Definition List",
    detail: "Flow Definition Detail",
    node_templetes: "Node Templates",
    opt: "operation",
    option: {
      versions: "Versions",
      showYaml: "ShowYaml",
      edit: "Edit",
      create: "Create"
    },
    columns: {
      create_time: "Create Time",
      update_time: "Update Time",
      business_id: "Business Name",
      feature_id: "Feature Name",
      alias: "Alias",
    },
    modal:{
      title: "Version Information",
      columns: {
        "descriptor_id": "Flow Id",
        "create_time": "Create Time"
      }
    },
    node_templates_detail: {
      opt: "operation",
      option: {
        disable: "Disable",
        enable: "Enable",
        edit: "Edit",
        create: "Create",
        preview_json_schema: "preview jsonSchema",
        editTask_template: "Edit Task Template",
        preview_input: "Preview Input",
        preview_output: "Preview Output",
      },
      columns: {
        name: "Template Name",
        type: "Template Type",
        node_type: "Node Type",
        category: "Category",
        status: "Status",
        disable: "Disable",
        enable: "Enable",
      },
      node_type: {
        meta: "Meta Data",
        template: "Template"
      },
      task_template_detail: {
        columns: {
          name: "Template Name",
          type: "Template Type",
          function_template: "Function Template",
          plugin_template: "Plugin Template",
          logic_template: "Logic Template",
          task_yaml: "Template tasks default to yaml",
          schema: "Template input structure (schema)",
          output: "Template output structure",
        }
      }
    }
  },
  instances: {
    record: "Execution Records",
    detail: "Execution Detail",
    record_detail: "Execution Records Detail",
    opt: "Operation",
    option: {
      detail: "Detail"
    },
    status: {
      SUCCEEDED: "SUCCEEDED",
      FAILED: "FAILED",
      READY: "READY",
      NOT_STARTED: "NOT_STARTED",
      RUNNING: "RUNNING",
      SKIPPED: "SKIPPED",
    },
    columns: {
      submit_time: "Submit Time ",
      execution_id: "Execution ID",
      business_id: "Business Name",
      feature_id: "Feature Name",
      status: "Status"
    },
    form_props: {
      business_id: "Business Name",
      business_placeholder: "Please select a business name",
      feature_id: "Feature Name",
      feature_placeholder: "The feature name is associated with the business name",
      execution_id: "Execution ID",
      time_range_select: "TimeRange",
      start_time_placeholder: "Start date and time",
      end_time_placeholder: "End date and time",
    },
    graph: {
      execution_detail_expire_message: "执行详情已过期",
      execution_detail_none_message: "请先到执行列表中选择要查看的记录",
      grid: {
        title: "Dag Execution Details",
        schema: {
          execution_id: "Execution ID",
          status: "Status",
          progress: "Progress",
          start_time: "Start Time",
          end_time: "End Time",
          context: "Context",
          context_key: "Key",
          context_value: "Value",
          error_result_msg: "Execution Exception Message",
          error_result_default_msg: "No Exception Message",
          trace: "Trace",
          trace_detail: "Detail",
        }
      },
      node: {
        title: "Node Details",
        schema: {
          name: "Node Name",
          category: "Category",
          resource_protocol: "Resource Protocol",
          resource_name: "Resource Name",
          pattern: "Synchronous",
          tolerance: "Failure Skip",
          start_time: "Start Time",
          end_time: "End Time",
          status: "Status",
          input_msg: "Input Msg",
          input_msg_key: "Key",
          input_msg_value: "Value",
          input_mappings: "Input Mappings",
          input_mappings_source: "Input Mappings Source",
          input_mappings_target: "Input Mappings Target",
          conditions: "Conditions",
          conditions_key: "Conditions List",
          output_mappings: "Ouput Mappings",
          iteration_msg: "iteration Msg",
          iteration_msg_key: "Key",
          iteration_msg_value: "Value",
          output_msg: "Output Msg",
          output_msg_key: "Key",
          output_msg_value: "Value",
          error_result_msg: "Execution Exception Message",
        }
      }
    }

  },
  trigger: {
    index: "Trigger",
    list: "Trigger List",
  }
}
