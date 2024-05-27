import {defineConfig, loadEnv} from 'vite';
import vue from '@vitejs/plugin-vue';
import qiankun from 'vite-plugin-qiankun';
import { fileURLToPath, URL } from 'node:url';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  console.log("micro app: ", mode, env.VITE_BASE_URL)
  return {
    define: {
      __APP_ENV__: JSON.stringify(env.APP_ENV),
    },
    base: env.VITE_BASE_URL,
    plugins: [
      vue(),
      qiankun('flow-graph', {
        useDevMode: true,
      }),
    ],
    server: {
      port: env.VITE_SERVER_PORT,
      headers: {
        'Access-Control-Allow-Origin': '*',
      },
    },
    resolve: {
      alias: {
        '@': fileURLToPath(new URL('./', import.meta.url)),
        'vue': 'vue/dist/vue.esm-bundler.js',
      },
      dedupe: [
        'vue'
      ]
    },
    optimizeDeps: {
      include: [
        '@iconify/iconify',
        'ant-design-vue/es/locale/zh_CN',
        'ant-design-vue/es/locale/en_US',
      ],
      exclude: ['@antv/x6-vue-shape']
    },
    css: {
      preprocessorOptions: {
        less: {
          javascriptEnabled: true,
        }
      },
    },
    extraBabelPlugins: [
      [
        'babel-plugin-import',
        { libraryName: 'antd', libraryDirectory: 'lib', style: true },
      ],
      [
        'babel-plugin-import',
        { libraryName: '@formily/antd', libraryDirectory: 'lib', style: true },
        'formilyAntd',
      ],
    ],
  }
});
