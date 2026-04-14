import { useSearchParams } from 'react-router-dom'

import { mergeSearchParams, type UrlStateValue } from './pageUrlState'

interface SearchStateClient<TParams> {
  readSearchParams: (searchParams: URLSearchParams) => TParams
  toSearchUpdates: (updates: Partial<TParams>) => Record<string, UrlStateValue>
}

export function usePageSearchState<TParams>(client: SearchStateClient<TParams>) {
  const [searchParams, setSearchParams] = useSearchParams()
  const params = client.readSearchParams(searchParams)

  const updateParams = (updates: Partial<TParams>) => {
    setSearchParams(mergeSearchParams(searchParams, client.toSearchUpdates(updates)), { replace: true })
  }

  return {
    params,
    updateParams,
  }
}
