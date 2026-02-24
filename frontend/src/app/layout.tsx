import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'BlinkQuant',
  description: 'Distributed Quant System',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      {/* 移除 bg-gray-950，使用 globals.css 中的浅色定义 */}
      <body>{children}</body>
    </html>
  )
}
