# build rill-flow-ui 
FROM node:18.12.1-alpine as build-stage

WORKDIR /data/rill-flow-ui
COPY ./rill-flow-ui/ ./

RUN npm config set registry https://registry.npm.taobao.org
RUN npm cache clean --force
RUN npm config set strict-ssl false

RUN npm install pnpm -g && pnpm install && pnpm build:docker

# build rill-flow-graph
FROM node:18.12.1-alpine as build-submodule-stage

WORKDIR /data/rill-flow-graph
COPY ./flow-graph ./

RUN npm config set registry https://registry.npm.taobao.org
RUN npm cache clean --force
RUN npm config set strict-ssl false

RUN npm install pnpm -g && pnpm install && pnpm build:docker

FROM nginx:1.23.3-alpine as production-stage
COPY --from=build-stage /data/rill-flow-ui/dist /usr/share/nginx/html/dist
COPY --from=build-stage /data/rill-flow-ui/deploy/nginx.conf /etc/nginx/nginx.conf.template
COPY --from=build-submodule-stage /data/rill-flow-graph/dist /usr/share/nginx/html/dist/flow-graph

CMD /bin/sh -c "envsubst '\$BACKEND_SERVER' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf && nginx -g 'daemon off;'"
