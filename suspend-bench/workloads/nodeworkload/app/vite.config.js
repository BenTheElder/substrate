// Vite config for the agent-edited sample app (lovable.dev-style).
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '127.0.0.1',
    port: 5173,
    strictPort: true,
    // No browser client connects inside the microVM; the agent loop drives
    // transforms via HTTP. Keep the file watcher (the realistic warm state).
    hmr: false,
  },
})
