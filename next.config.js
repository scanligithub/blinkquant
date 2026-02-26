/** @type {import('next').NextConfig} */
const nextConfig = {
  webpack: (config, { isServer }) => {
    // For client-side bundles, alias 'fs' to false
    if (!isServer) {
      config.resolve.fallback = {
        fs: false,
        path: false, // Often 'path' is also a common Node.js module that causes issues
        crypto: false, // 'crypto' can also be problematic
      };
    }
    return config;
  },
};

module.exports = nextConfig;
