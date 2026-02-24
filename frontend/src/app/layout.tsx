import type { Metadata } from 'next'
import './globals.css' // <--- 必须添加这一行！

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
      <body className="bg-gray-950 text-slate-200">{children}</body>
    </html>
  )
}
