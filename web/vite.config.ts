import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// During development, proxy billing API calls to the billing service.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/v1': 'http://localhost:8080',
    },
  },
})
