/** @type {import('next').NextConfig} */
const nextConfig = {
  async rewrites() {
    return [
      // 这里配置反向代理，让前端 /api/node0 直接转发到 HF 节点
      {
        source: '/api/node0/:path*',
        destination: 'https://scanli-blinkquant-node1.hf.space/:path*',
      },
      {
        source: '/api/node1/:path*',
        destination: 'https://scanli-blinkquant-node2.hf.space/:path*',
      },
      {
        source: '/api/node2/:path*',
        destination: 'https://scanli-blinkquant-node3.hf.space/:path*',
      },
    ]
  },
}

module.exports = nextConfig
