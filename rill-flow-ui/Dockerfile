FROM node:16-alpine as build-stage

WORKDIR /data/rill-flow-ui
COPY . ./

# 设置 node 阿里镜像
RUN npm config set registry https://registry.npm.taobao.org

# 设置阿里镜像、pnpm、依赖、编译
RUN npm install pnpm -g && pnpm install && pnpm build:prod
# node部分结束

RUN echo "🎉 编 🎉 译 🎉 成 🎉 功 🎉"

FROM nginx:1.23.3-alpine as production-stage
COPY --from=build-stage /data/rill-flow-ui/dist /usr/share/nginx/html/dist
COPY --from=build-stage /data/rill-flow-ui/deploy/nginx.conf /etc/nginx/nginx.conf.template

RUN echo "🎉  启动服务  🎉"

# 启动 Nginx 服务
# 在容器启动时替换 Nginx 配置文件中的模板变量
CMD /bin/sh -c "envsubst '\$BACKEND_SERVER' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf && nginx -g 'daemon off;'"
RUN echo "🎉 架 🎉 设 🎉 成 🎉 功 🎉"

