export default function Home() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-between p-24">
      <div className="z-10 max-w-5xl w-full items-center justify-between font-mono text-sm lg:flex">
        <p className="fixed left-0 top-0 flex w-full justify-center border-b border-gray-300 bg-gradient-to-b from-zinc-200 pb-6 pt-8 backdrop-blur-2xl dark:border-neutral-800 dark:bg-zinc-800/30 dark:from-inherit lg:static lg:w-auto  lg:rounded-xl lg:border lg:bg-gray-200 lg:p-4 lg:dark:bg-zinc-800/30">
          BlinkQuant Control Panel
        </p>
      </div>
      <div className="text-center">
        <h1 className="text-4xl font-bold">System Status: Initializing...</h1>
        <p className="mt-4 text-gray-500">Node Cluster: 3/3 Online</p>
      </div>
    </main>
  )
}
