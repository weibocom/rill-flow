/**
 * 事件分发类型
 * @enum
 */
export const CustomEventTypeEnum = {
    /**@type {String} 单元点击触发 */
    CELL_CLICK: '__antv_x6_custom_event_type_cell_click__',
    /**@type {String} 节点点击触发 */
    NODE_CLICK: '__antv_x6_custom_event_type_node_click__',
    /**@type {String} 双击节点触发 */
    DOUBLE_NODE_CLICK: '__antv_x6_custom_event_type_cell_double_click__',
    EDGE_ADD: '__edge_add__',
    NODE_ADD: '__node_add__',
    NODE_REMOVE: '__node_remove__',
    EDGE_REMOVE: '__edge_remove__',
    /**@type {String} 帮助信息 */
    HELP: '__antv_x6_custom_event_type_help__',
    /**@type {String} 冻结画布 */
    FREEZE_GRAPH: '__antv_x6_custom_event_type_freeze_graph__',
    /**@type {String} 运行时异常 */
    RUNTIME_ERR: '__antv_x6_custom_event_type_runtime_err__',
    /**@type {String} 工具栏显示DAG信息 */
    TOOL_BAR_SHOW_DAG: '__tool_bar_show_dag__',
    /**@type {String} 工具栏保存DAG信息 */
    TOOL_BAR_SAVE_DAG: '__tool_bar_save_dag__',
    /**@type {String} 工具栏编辑DAG InputSchema信息 */
    TOOL_BAR_EDIT_INPUT_SCHEMA: '__tool_bar_edit_input_schema__',
    /**@type {String} 工具栏编辑DAG提交测试任务 */
    TOOL_BAR_SUBMIT_DAG_TEST_RUN: '__tool_bar_submit_test_run__',
    /**@type {String} 工具栏DAG执行详情查看 */
    TOOL_BAR_EXECUTION_DETAIL: '__tool_bar_execution_detail__',
    /**@type {String} 展示执行详情 */
    SHOW_EXECUTION_RESULT: '__show_execution_result__',
    /**@type {String} 刷新graph */
    REFRESH_DAG_GRAPH: '__refresh_dag_graph__',
}

