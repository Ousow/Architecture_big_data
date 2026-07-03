import { useState } from 'react'
import SearchView from './features/search/SearchView'
import FicheEntreprise from './features/entreprise/FicheEntreprise'

export default function App() {
  const [selectedNumero, setSelectedNumero] = useState(null)

  return (
    <div className="app">
      <h1 style={{ fontSize: 22 }}>KBO Hôtellerie</h1>
      {selectedNumero ? (
        <FicheEntreprise numero={selectedNumero} onBack={() => setSelectedNumero(null)} />
      ) : (
        <SearchView onSelect={setSelectedNumero} />
      )}
    </div>
  )
}
