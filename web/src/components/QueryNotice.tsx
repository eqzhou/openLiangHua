interface QueryNoticeProps {
  isLoading?: boolean
  error?: unknown
  loadingText?: string
}

function errorMessage(error: unknown): string {
  if (error instanceof Error && error.message) {
    return error.message
  }
  return '请求失败，请稍后重试。'
}

export function QueryNotice({ isLoading, error, loadingText = '正在加载最新数据...' }: QueryNoticeProps) {
  if (error) {
    return (
      <div className="query-notice query-notice--error" role="status">
        {errorMessage(error)}
      </div>
    )
  }

  if (isLoading) {
    return (
      <div className="query-notice query-notice--loading" role="status">
        {loadingText}
      </div>
    )
  }

  return null
}
