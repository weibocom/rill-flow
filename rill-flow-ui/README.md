## Introduction

This is a description of Rill Flow UI frontend development based on [Vue vben admin](https://github.com/vbenjs/vue-vben-admin).

## Prerequisites

- [node](http://nodejs.org/) and [git](https://git-scm.com/) -Development environment setup
- [Vite](https://vitejs.dev/) - Familiarity with Vite features
- [Vue3](https://v3.vuejs.org/) - Understanding of Vue basics
- [TypeScript](https://www.typescriptlang.org/) - Familiarity with TypeScript syntax
- [Es6+](http://es6.ruanyifeng.com/) - Understanding of ES6 syntax
- [Vue-Router-Next](https://next.router.vuejs.org/) - Familiarity with basic usage of Vue Router
- [Ant-Design-Vue](https://2x.antdv.com/docs/vue/introduce-cn/) - Basic usage of UI components
- [Mock.js](https://github.com/nuysoft/Mock) - Familiarity with Mock.js syntax
- [QianKun](https://qiankun.umijs.org/zh/guide) - Basic usage of QianKun


## Installation and Usage
- Install the Node Version Manager [nvm](https://github.com/nvm-sh/nvm), a tool for managing node.js versions:

```shell
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.37.2/install.sh | bash
```

- Install Node.js version v18.19.0 and set it as the default global version:

```shell
nvm install v18.19.0
nvm alias default v18.19.0
```

- Install pnpm:

```shell
npm install -g pnpm --registry=https://registry.npmmirror.com
```

- Install dependencies:
```bash
cd rill-flow-ui

pnpm install

```

- Run:

```bash
pnpm serve
```

- Build:

```bash
pnpm build:docker
```
