## Introduction

Flow Graph Frontend is developed based on [Vue3](https://v3.vuejs.org/) and [Graph Editing Engine x6](https://x6.antv.antgroup.com/).

## Prerequisites

- [Node](http://nodejs.org/) and [Git](https://git-scm.com/) - Project development environment 
- [Vite](https://vitejs.dev/)  - Familiarity with Vite features 
- [Vue3](https://v3.vuejs.org/) - Familiarity with Vue basic syntax 
- [TypeScript](https://www.typescriptlang.org/) - Familiarity with basic TypeScript syntax 
- [Es6+](http://es6.ruanyifeng.com/) - Familiarity with basic ES6 syntax 
- [Ant-Design-Vue](https://2x.antdv.com/docs/vue/introduce-cn/) - Basic usage of Ant Design Vue UI library 
- [Graph Editing Engine X6](https://x6.antv.antgroup.com/) - Familiarity with basic X6 syntax
- [QianKun](https://qiankun.umijs.org/zh/guide) - Basic usage of QianKun

## Installation and Usage

- Install Node version manager nvm[nvm](https://github.com/nvm-sh/nvm)

```shell
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.37.2/install.sh | bash
```

- Install Node version v18.19.0 and set it as the global default version

```shell
nvm install v18.19.0
nvm alias default v18.19.0
```

- Install pnpm

```shell
npm install -g pnpm --registry=https://registry.npmmirror.com
```

- Install dependencies
```bash
cd flow-graph

pnpm install

```

- Run

```bash
pnpm serve
```

- Build

```bash
pnpm build:docker
```
