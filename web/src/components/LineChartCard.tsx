import type { ReactNode } from 'react'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { JsonRecord } from '../types/api'

interface LineChartCardProps {
  data: JsonRecord[]
  xKey: string
  lineKeys?: string[]
  title?: string
  subtitle?: string
  actions?: ReactNode
  emptyText?: string
}

const palette = ['#c2563f', '#0e5a8a', '#537d5d', '#9c7a2f', '#7c4d79']

function resolveLineKeys(data: JsonRecord[], xKey: string, lineKeys?: string[]): string[] {
  if (lineKeys && lineKeys.length) {
    return lineKeys
  }
  const firstRow = data[0] ?? {}
  return Object.keys(firstRow).filter((key) => key !== xKey)
}

export function LineChartCard({
  data,
  xKey,
  lineKeys,
  title,
  subtitle,
  actions,
  emptyText = '暂无可展示曲线',
}: LineChartCardProps) {
  const resolvedLineKeys = resolveLineKeys(data, xKey, lineKeys)

  if (!data.length || !resolvedLineKeys.length) {
    return <div className="empty-state">{emptyText}</div>
  }

  return (
    <figure className="chart-card">
      {title ? (
        <figcaption className="chart-card__header">
          <div className="chart-card__copy">
            <h3 className="chart-card__title">{title}</h3>
            {subtitle ? <p className="chart-card__subtitle">{subtitle}</p> : null}
          </div>
          {actions ? <div className="chart-card__actions">{actions}</div> : null}
        </figcaption>
      ) : null}
      <div className="chart-card__canvas">
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(70, 52, 30, 0.12)" />
            <XAxis dataKey={xKey} tick={{ fill: '#5f5648', fontSize: 12 }} minTickGap={24} />
            <YAxis tick={{ fill: '#5f5648', fontSize: 12 }} width={72} />
            <Tooltip />
            <Legend />
            {resolvedLineKeys.map((lineKey, index) => (
              <Line
                key={lineKey}
                type="monotone"
                dataKey={lineKey}
                stroke={palette[index % palette.length]}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4 }}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </figure>
  )
}
