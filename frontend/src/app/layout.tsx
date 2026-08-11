import type { Metadata } from 'next'
import { Inter, JetBrains_Mono } from 'next/font/google'
import './globals.css'
import { IdleTimeoutProvider } from '@/components/IdleTimeoutProvider'

const inter = Inter({
  subsets: ['latin'],
  display: 'swap',
  variable: '--font-inter',
})

const mono = JetBrains_Mono({
  subsets: ['latin'],
  display: 'swap',
  variable: '--font-mono',
})

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
    <html lang="zh-CN" className={`${inter.variable} ${mono.variable}`}>
      <head><script dangerouslySetInnerHTML={{ __html: themeScript }} /></head>
      <body className="font-sans bg-bg text-ink antialiased">
        <IdleTimeoutProvider>
          {children}
        </IdleTimeoutProvider>
      </body>
    </html>
  )
}
