import React from 'react'
import generated from './components/generated.jsx'
import Counter from './components/Counter.jsx'

// The app a Claude agent is iterating on: a Counter component the agent edits
// continuously, plus a graph of generated components (a realistic module graph
// for Vite to keep warm).
export default function App() {
  return (
    <main>
      <h1>Lovable-style app (agent-edited)</h1>
      <Counter />
      {generated.map((C, i) => <C key={i} />)}
    </main>
  )
}
