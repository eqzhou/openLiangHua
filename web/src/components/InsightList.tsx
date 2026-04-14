interface InsightListProps {
  title: string
  items: string[]
  emptyText?: string
  tone?: 'default' | 'good' | 'warn'
}

export function InsightList({ title, items, emptyText = '暂无要点', tone = 'default' }: InsightListProps) {
  return (
    <section className={`insight-card insight-card--${tone}`}>
      <h3 className="insight-card__title">{title}</h3>
      {items.length ? (
        <ul className="insight-card__list">
          {items.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
      ) : (
        <p className="helper-text">{emptyText}</p>
      )}
    </section>
  )
}
