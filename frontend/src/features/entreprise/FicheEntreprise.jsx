import { useEffect } from 'react'
import { useDispatch, useSelector } from 'react-redux'
import { loadEntreprise, clearEntreprise, setSelectedYear } from './entrepriseSlice'
import ResultatSankey from '../../components/ResultatSankey'

function formatEuro(v) {
  if (v == null) return '—'
  return Math.round(v).toLocaleString('fr-BE') + ' €'
}

function formatPct(v) {
  if (v == null) return '—'
  return v.toFixed(1) + ' %'
}

function signClass(v) {
  if (v == null) return ''
  return v >= 0 ? 'positive' : 'negative'
}

export default function FicheEntreprise({ numero, onBack }) {
  const dispatch = useDispatch()
  const { data, selectedYear, status } = useSelector((s) => s.entreprise)

  useEffect(() => {
    dispatch(loadEntreprise(numero))
    return () => dispatch(clearEntreprise())
  }, [numero, dispatch])

  if (status === 'loading') return <p>Chargement...</p>
  if (status === 'failed') return <p>Entreprise introuvable.</p>
  if (!data) return null

  const { identity, financials } = data
  const denomination = identity.denominations?.[0]?.Denomination || '(sans nom)'
  const address = identity.addresses?.[0]
  const years = financials.years || []
  const currentYearData = years.find((y) => y.year === selectedYear)
  const ratios = currentYearData?.ratios

  return (
    <div>
      <span className="back-link" onClick={onBack}>← Retour à la recherche</span>

      <div className="card entreprise-header">
        <h2>{denomination}</h2>
        <p className="subline">
          {identity.enterprise_number} · {identity.juridical_form_label} · {identity.status_label}
        </p>
        {address && (
          <p className="address">
            {address.StreetFR} {address.HouseNumber}, {address.Zipcode} {address.MunicipalityFR}
          </p>
        )}
        <p className="since">En activité depuis le {identity.start_date}</p>
      </div>

      <div className="card">
        <h3 style={{ marginTop: 0, marginBottom: 14, fontSize: 16 }}>Activités (NACE)</h3>
        <ul className="activities-list">
          {identity.activities.map((a, i) => (
            <li key={i}>
              <span className={`badge ${a.Classification === 'MAIN' ? 'main' : 'seco'}`}>
                {a.Classification}
              </span>
              {a.NaceLabel} <span className="nace-code">({a.NaceCode}, {a.NaceVersion})</span>
            </li>
          ))}
        </ul>
      </div>

      {!financials.available && (
        <div className="card">
          <p style={{ margin: 0, color: '#6b7280' }}>
            Aucune donnée financière disponible pour cette entreprise (hors périmètre hôtellier scrapé).
          </p>
        </div>
      )}

      {financials.available && years.length > 0 && (
        <>
          <div className="year-selector">
            {years.map((y) => (
              <button
                key={y.year}
                className={`year-btn ${y.year === selectedYear ? 'active' : ''}`}
                onClick={() => dispatch(setSelectedYear(y.year))}
              >
                {y.year}
              </button>
            ))}
          </div>

          <ResultatSankey yearData={currentYearData} />

          <div className="card">
            <h3 style={{ marginTop: 0, marginBottom: 14, fontSize: 16 }}>Ratios financiers — {selectedYear}</h3>
            <table className="ratios-table">
              <tbody>
                <tr>
                  <th>EBIT</th>
                  <td className={signClass(currentYearData?.ebit)}>{formatEuro(currentYearData?.ebit)}</td>
                </tr>
                <tr>
                  <th>Résultat net</th>
                  <td className={signClass(currentYearData?.resultat_net)}>{formatEuro(currentYearData?.resultat_net)}</td>
                </tr>
                <tr>
                  <th>Fonds propres</th>
                  <td className={signClass(currentYearData?.fonds_propres)}>{formatEuro(currentYearData?.fonds_propres)}</td>
                </tr>
                <tr>
                  <th>Marge nette</th>
                  <td className={signClass(ratios?.marge_nette_pct)}>{formatPct(ratios?.marge_nette_pct)}</td>
                </tr>
                <tr>
                  <th>ROE</th>
                  <td className={signClass(ratios?.roe_pct)}>{formatPct(ratios?.roe_pct)}</td>
                </tr>
                <tr>
                  <th>Ratio de liquidité</th>
                  <td>{ratios?.ratio_liquidite?.toFixed(2) ?? '—'}</td>
                </tr>
                <tr>
                  <th>Taux d'endettement</th>
                  <td>{formatPct(ratios?.taux_endettement_pct)}</td>
                </tr>
              </tbody>
            </table>
            <p className="schema-note">Schéma de dépôt : {financials.schema_type}</p>
          </div>
        </>
      )}
    </div>
  )
}