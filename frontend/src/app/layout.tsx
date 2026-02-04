import type { Metadata } from 'next'

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
      <body>{children}</body>
    </html>
  )
}
