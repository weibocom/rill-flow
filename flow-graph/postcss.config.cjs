module.exports = (ctx) => {
  return {
    plugins: {
      'postcss-selector-namespace': {
        namespace(css) {
          // 不需要添加命名空间的文件
          if (css.includes('antd.css')) return '';
          if (css.includes('formily')) return '';
          if (css.includes('codemirror')) return '';
          return '#flow-graph'
        }
      }
    }
  }
}

