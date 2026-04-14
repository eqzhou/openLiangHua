import { useEffect, useMemo, useState, type PropsWithChildren } from 'react'

import { resolveInitialTheme, writeThemeMode, type ThemeMode } from '../lib/uiPreferences'
import { ThemeContext } from './themeContext'

export function ThemeProvider({ children }: PropsWithChildren) {
  const [theme, setTheme] = useState<ThemeMode>(() => resolveInitialTheme())

  useEffect(() => {
    document.documentElement.dataset.theme = theme
    document.documentElement.style.colorScheme = theme
    writeThemeMode(theme)
  }, [theme])

  const contextValue = useMemo(
    () => ({
      theme,
      setTheme,
      toggleTheme: () => setTheme((current) => (current === 'light' ? 'dark' : 'light')),
    }),
    [theme],
  )

  return <ThemeContext.Provider value={contextValue}>{children}</ThemeContext.Provider>
}
