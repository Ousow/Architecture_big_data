import { Sankey, Tooltip, ResponsiveContainer, Rectangle, Layer } from 'recharts'

const COLOR_CA = '#1E2761'     // navy — chiffre d'affaires, neutre
const COLOR_MARGE = '#B8860B'  // ambre — marge brute, après déduction des coûts
const COLOR_PROFIT = '#0F9D58' // vert — résultat net positif
const COLOR_PERTE = '#D93025'  // rouge — résultat net négatif

function euro(v) {
  return Math.round(v).toLocaleString('fr-BE') + ' €'
}

function LabeledNode({ x, y, width, height, index, payload }) {
  const anchor = index === 0 ? 'start' : index === 2 ? 'end' : 'middle'
  const textX = index === 0 ? x : index === 2 ? x + width : x + width / 2

  return (
    <Layer>
      <Rectangle x={x} y={y} width={width} height={height} fill={payload.fill} radius={2} />
      <text x={textX} y={y - 10} textAnchor={anchor} fontSize={13} fontWeight={600} fill="#1A1A2E">
        {payload.name}
      </text>
    </Layer>
  )
}

function ColoredLink(resultColor) {
  return function CustomLink(props) {
    const { sourceX, sourceY, sourceControlX, targetX, targetY, targetControlX, linkWidth, index } = props
    const color = index === 0 ? COLOR_MARGE : resultColor
    const path = `M${sourceX},${sourceY}C${sourceControlX},${sourceY} ${targetControlX},${targetY} ${targetX},${targetY}`
    return <path d={path} fill="none" stroke={color} strokeOpacity={0.35} strokeWidth={linkWidth} />
  }
}

/**
 * Sankey à 3 nœuds fixes, quel que soit le schéma de dépôt (full/abrégé/micro) :
 *   CA (chiffre_affaires) -> Marge brute -> Résultat net
 * Couleurs sémantiques : le flux CA -> Marge brute est en ambre (coûts déduits
 * en chemin), le flux Marge brute -> Résultat net est vert si le résultat est
 * positif, rouge s'il est négatif (perte).
 * Si le CA n'est pas disponible (schéma abrégé/micro sans code 70), on ne peut
 * pas tracer le premier flux — on affiche un message plutôt qu'un graphe faux.
 */
export default function ResultatSankey({ yearData }) {
  if (!yearData) return null

  const { chiffre_affaires, resultat_net } = yearData
  const marge_brute = yearData.ratios?.marge_brute

  if (chiffre_affaires == null || marge_brute == null) {
    return (
      <div className="card">
        <p style={{ margin: 0, color: '#6b7280' }}>
          Chiffre d'affaires non disponible pour cet exercice (dépôt en schéma abrégé/micro) —
          le Sankey nécessite ce montant pour être tracé.
        </p>
      </div>
    )
  }

  const isProfit = (resultat_net ?? 0) >= 0
  const resultColor = isProfit ? COLOR_PROFIT : COLOR_PERTE

  const data = {
    nodes: [
      { name: `CA : ${euro(chiffre_affaires)}`, fill: COLOR_CA },
      { name: `Marge brute : ${euro(marge_brute)}`, fill: COLOR_MARGE },
      { name: `Résultat net : ${resultat_net != null ? euro(resultat_net) : '—'}`, fill: resultColor },
    ],
    links: [
      { source: 0, target: 1, value: Math.max(Math.abs(marge_brute), 1) },
      { source: 1, target: 2, value: Math.max(Math.abs(resultat_net ?? 0), 1) },
    ],
  }

  return (
    <div className="card">
      <ResponsiveContainer width="100%" height={220}>
        <Sankey
          data={data}
          node={<LabeledNode />}
          link={ColoredLink(resultColor)}
          nodePadding={40}
          margin={{ left: 10, right: 10, top: 24, bottom: 10 }}
        >
          <Tooltip />
        </Sankey>
      </ResponsiveContainer>
      <div className="sankey-legend">
        <span><span className="dot" style={{ background: COLOR_CA }} /> Chiffre d'affaires</span>
        <span><span className="dot" style={{ background: COLOR_MARGE }} /> Marge brute (coûts déduits)</span>
        <span><span className="dot" style={{ background: resultColor }} /> Résultat net {isProfit ? '(bénéfice)' : '(perte)'}</span>
      </div>
    </div>
  )
}