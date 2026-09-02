import type { Metadata } from 'next'
import '@fontsource/inter/400.css'
import '@fontsource/inter/500.css'
import '@fontsource/inter/600.css'
import '@fontsource/inter/700.css'
import '@fontsource/inter/800.css'
import '@fontsource/inter/900.css'
import '@fontsource/jetbrains-mono/400.css'
import '@fontsource/jetbrains-mono/500.css'
import '@fontsource/jetbrains-mono/700.css'
import './globals.css'
import { IdleTimeoutProvider } from '@/components/IdleTimeoutProvider'

export const metadata: Metadata = {
  title: 'BlinkQuant · 分布式量化交易',
  description: 'Distributed Quant System',
}

const themeScript = `
(function(){try{
  var s=localStorage.getItem('bq-theme');
  var m=window.matchMedia('(prefers-color-scheme: dark)').matches;
  var t=s||(m?'dark':'light');
  document.documentElement.classList.add(t);
  document.documentElement.setAttribute('data-theme',t);
}catch(e){}})();
`

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="zh-CN">
      <head><script dangerouslySetInnerHTML={{ __html: themeScript }} /></head>
      <body className="font-sans bg-bg text-ink antialiased">
        <IdleTimeoutProvider>
          {children}
        </IdleTimeoutProvider>
      </body>
    </html>
  )
}
