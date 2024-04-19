module.exports = (ctx) => {
  return {
    plugins: [
      require('postcss-plugin-namespace')('#htmlRoot', {
        ignore: [
          /body/,
          /html/,
          '#app'
        ]
      }),
    ]
  }
}

