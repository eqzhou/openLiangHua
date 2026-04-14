export class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function parseResponseError(response: Response): Promise<never> {
  let detail = ''

  try {
    const contentType = response.headers.get('content-type') ?? ''
    if (contentType.includes('application/json')) {
      const payload = (await response.json()) as { detail?: unknown; error?: { message?: unknown } }
      const jsonDetail =
        typeof payload.detail === 'string'
          ? payload.detail
          : typeof payload.error?.message === 'string'
            ? payload.error.message
            : ''
      detail = jsonDetail.trim()
    } else {
      detail = (await response.text()).trim()
    }
  } catch {
    detail = ''
  }

  throw new ApiError(response.status, detail || `Request failed: ${response.status}`)
}

async function parseJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    return parseResponseError(response)
  }
  return (await response.json()) as T
}

export async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(path, {
    credentials: 'include',
    headers: {
      Accept: 'application/json',
    },
  })
  return parseJson<T>(response)
}

export async function apiPost<T>(path: string, payload?: unknown): Promise<T> {
  const response = await fetch(path, {
    method: 'POST',
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: payload === undefined ? undefined : JSON.stringify(payload),
  })
  return parseJson<T>(response)
}

export async function apiPut<T>(path: string, payload: unknown): Promise<T> {
  const response = await fetch(path, {
    method: 'PUT',
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(payload),
  })
  return parseJson<T>(response)
}
