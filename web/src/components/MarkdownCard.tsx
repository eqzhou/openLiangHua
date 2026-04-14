import Markdown from 'react-markdown'

interface MarkdownCardProps {
  title: string
  content?: string
}

export function MarkdownCard({ title, content }: MarkdownCardProps) {
  if (!content) {
    return <div className="empty-state">暂无内容</div>
  }

  return (
    <article className="markdown-card">
      <h3 className="markdown-card__title">{title}</h3>
      <Markdown>{content}</Markdown>
    </article>
  )
}
