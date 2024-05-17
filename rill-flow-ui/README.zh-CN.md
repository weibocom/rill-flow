## 简介

Rill Flow UI 前端基于 [Vue vben admin](https://github.com/vbenjs/vue-vben-admin) 开发

## 准备

- [node](http://nodejs.org/) 和 [git](https://git-scm.com/) -项目开发环境
- [Vite](https://vitejs.dev/) - 熟悉 vite 特性
- [Vue3](https://v3.vuejs.org/) - 熟悉 Vue 基础语法
- [TypeScript](https://www.typescriptlang.org/) - 熟悉`TypeScript`基本语法
- [Es6+](http://es6.ruanyifeng.com/) - 熟悉 es6 基本语法
- [Vue-Router-Next](https://next.router.vuejs.org/) - 熟悉 vue-router 基本使用
- [Ant-Design-Vue](https://2x.antdv.com/docs/vue/introduce-cn/) - ui 基本使用
- [QianKun](https://qiankun.umijs.org/zh/guide) - 熟悉 QianKun 基本使用

## 安装使用
- 安装 node 版本管理工具[nvm](https://github.com/nvm-sh/nvm)

```shell
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.37.2/install.sh | bash
```

- 安装版本为 v18.19.0 的 node，并设定为全局默认版本

```shell
nvm install v18.19.0
nvm alias default v18.19.0
```

- 安装pnpm

```shell
npm install -g pnpm --registry=https://registry.npmmirror.com
```

- 安装依赖
```bash
cd rill-flow-ui

pnpm install

```

- 运行

```bash
pnpm serve
```

- 打包

```bash
pnpm build:prod
```
