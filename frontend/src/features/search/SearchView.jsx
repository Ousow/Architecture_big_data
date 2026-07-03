import { useDispatch, useSelector } from 'react-redux'
import { setQuery, runSearch, clearResults } from './searchSlice'

export default function SearchView({ onSelect }) {
  const dispatch = useDispatch()
  const { query, results, status } = useSelector((s) => s.search)

  function handleChange(e) {
    const value = e.target.value
    dispatch(setQuery(value))
    if (value.trim().length >= 2) {
      dispatch(runSearch(value.trim()))
    } else {
      dispatch(clearResults())
    }
  }

  return (
    <div>
      <input
        className="search-bar"
        placeholder="Rechercher par nom ou numéro BCE..."
        value={query}
        onChange={handleChange}
        autoFocus
      />

      {status === 'loading' && <p>Recherche...</p>}
      {status === 'failed' && <p>Erreur pendant la recherche.</p>}

      <ul className="result-list">
        {results.map((r) => (
          <li key={r.enterprise_number} className="result-item" onClick={() => onSelect(r.enterprise_number)}>
            <div>
              <div className="denom">{r.denomination || '(sans nom)'}</div>
              <div className="meta">
                {r.enterprise_number} · {r.commune || '—'} · {r.juridical_form_label || ''}
              </div>
            </div>
            <span className="meta">{r.status_label}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}
