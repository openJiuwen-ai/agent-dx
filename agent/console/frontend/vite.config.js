import {defineConfig} from 'vite';
export default defineConfig({
  base:'/console/',
  server:{proxy:{'/api':{target:'http://127.0.0.1:8088',ws:true}}},
  build:{rollupOptions:{output:{entryFileNames:'console.js',assetFileNames:asset=>asset.names?.some(name=>name.endsWith('.css'))?'console.css':'[name][extname]'}}},
});
