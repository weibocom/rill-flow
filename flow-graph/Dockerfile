FROM node:18.12.1-alpine as build-stage

WORKDIR /data/flow-graph
COPY . ./

RUN npm config set registry https://registry.npm.taobao.org
RUN npm cache clean --force
RUN npm config set strict-ssl false

RUN npm install pnpm -g && pnpm install && pnpm build:docker

FROM nginx:1.23.3-alpine as production-stage
COPY --from=build-stage /data/flow-graph/dist /usr/share/nginx/html/dist
COPY --from=build-stage /data/flow-graph/deploy/nginx.conf /etc/nginx/nginx.conf.template

CMD /bin/sh -c "envsubst '80' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf && nginx -g 'daemon off;'"
