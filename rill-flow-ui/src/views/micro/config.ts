export default {
  flowGraphApp: {
    name: 'flow-graph', // 子应用名称，跟package.json一致
    entry: import.meta.env.MODE === 'development' ? import.meta.env.VITE_FLOW_GRAPH_ENTRY : "/flow-graph/", // 子应用入口，本地环境下指定端口
    container: '#sub-container', // 挂载子应用的dom
    activeRule: '/micro/graph', // 路由匹配规则
    props: {}, // 主应用与子应用通信传值
  },
};
