# -*- coding: utf-8 -*-
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

with open('src/app/page.tsx', 'rb') as f:
    content = f.read()

# The old section in bytes (as it appears in the UTF-8 file)
old = b"""            <div className="flex flex-col gap-3">
            <div className="flex justify-center">
              <div className="text-xs md:text-sm font-mono text-slate-400 bg-white px-3 py-2 rounded-lg border shadow-sm">
                \xe9\x9b\x86\xe7\xbe\xa4: {clusterStatus?.cluster_health || '\xe8\xbf\x9e\xe6\x8e\xa5\xe4\xb8\xad...'}
              </div>
            </div>
            <div className="flex flex-wrap justify-center gap-3">
              {clusterStatus?.nodes?.map((node: any, idx: number) => (
                <div key={idx} className={`text-xs md:text-sm font-mono px-3 py-2 rounded-lg border shadow-sm ${node.online ? 'bg-white border-slate-200' : 'bg-red-50 border-red-200'}`}>
                  <div className="flex items-center gap-2">
                    <div className={`w-2.5 h-2.5 md:w-3 md:h-3 rounded-full ${node.online ? 'bg-green-500' : 'bg-red-500'}`}></div>
                    <span className="font-bold text-slate-700 text-sm md:text-base">Node {node.node || idx}</span>
                    <span className={`text-xs md:text-sm uppercase font-bold px-2 md:px-2.5 py-0.5 rounded-full ${
                      node.status === 'healthy' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'
                    }`}>
                      {node.status || 'OFFLINE'}
                    </span>
                  </div>
                  {node.online ? (
                    <div className="mt-2 space-y-1">
                      <div className="flex justify-between gap-3">
                        <span className="text-slate-500 text-xs md:text-sm">\xe8\xbf\x9b\xe7\xa8\x8b\xe5\x86\x85\xe5\xad\x98</span>
                        <div className="font-mono font-medium text-slate-900 text-right text-xs md:text-sm">{node.process_memory_gb} GB</div>
                      </div>
                      <div className="flex justify-between gap-3">
                        <span className="text-slate-500 text-xs md:text-sm">\xe7\xb3\xbb\xe7\x9b\x9f\xe7\xa9\xba\xe9\x97\xb2</span>
                        <div className="font-mono font-bold text-blue-600 text-right text-xs md:text-sm">{node.system_memory_free_gb} GB</div>
                      </div>
                      <div className="flex justify-between gap-3">
                        <span className="text-slate-500 text-xs md:text-sm">\xe7\xa3\x85\xe7\x9b\x94\xe7\xa9\xba\xe9\x97\xb2</span>
                        <div className="font-mono font-bold text-slate-900 text-right text-xs md:text-sm">{node.disk_free_gb} GB</div>
                      </div>
                      <div className="flex justify-between gap-3">
                        <span className="text-slate-500 text-xs md:text-sm">\xe6\x95\xb0\xe6\x8d\xae\xe8\xa1\x8c</span>
                        <div className="font-mono text-slate-500 text-right text-xs md:text-sm">{node.rows_daily?.toLocaleString()}</div>
                      </div>
                    </div>
                  ) : null}
                </div>
              ))}
            </div>"""

new = """            <div className="flex flex-col gap-3">
            <div className="flex justify-center">
              <div className="text-xs md:text-sm font-mono text-slate-400 bg-white px-3 py-2 rounded-lg border shadow-sm">
                \xe9\x9b\x86\xe7\xbe\xa4: {clusterStatus?.queueStats ? (clusterStatus.queueStats.running > 0 ? '\xe8\xbf\x90\xe8\xa1\x8c\xe4\xb8\xad' : '\xe7\xa9\xba\xe9\x97\xb2') : '\xe8\xbf\x9e\xe6\x8e\xa5\xe4\xb8\xad...'}
              </div>
            </div>
            <div className="flex flex-wrap justify-center gap-3">
              {clusterStatus?.nodes?.map((node: any, idx: number) => (
                <div
                  key={idx}
                  className={`text-xs md:text-sm font-mono px-3 py-2 rounded-lg border shadow-sm ${
                    node.status === 'idle' ? 'bg-green-50 border-green-200' :
                    node.status === 'running' ? 'bg-blue-50 border-blue-200' :
                    node.status === 'unhealthy' ? 'bg-red-50 border-red-200' :
                    'bg-gray-50 border-gray-200'
                  }`}
                >
                  <div className="flex items-center gap-2">
                    <div className={`w-2.5 h-2.5 md:w-3 md:h-3 rounded-full ${
                      node.status === 'idle' ? 'bg-green-500' :
                      node.status === 'running' ? 'bg-blue-500' :
                      node.status === 'unhealthy' ? 'bg-red-500' : 'bg-gray-500'
                    }`}></div>
                    <span className="font-bold text-slate-700 text-sm md:text-base">{node.name}</span>
                    <span className={`text-xs md:text-sm uppercase font-bold px-2 md:px-2.5 py-0.5 rounded-full ${
                      node.status === 'idle' ? 'bg-green-100 text-green-700' :
                      node.status === 'running' ? 'bg-blue-100 text-blue-700' :
                      node.status === 'unhealthy' ? 'bg-red-100 text-red-700' :
                      'bg-gray-100 text-gray-700'
                    }`}>
                      {node.status === 'idle' ? '\xe7\xa9\xba\xe9\x97\xb2' : node.status === 'running' ? '\xe8\xbf\x90\xe8\xa1\x8c\xe4\xb8\xad' : node.status}
                    </span>
                  </div>
                  {node.status === 'running' and node.task_type and (
                    <div className="mt-2 space-y-1 text-xs">
                      <span>\xe4\xbb\xbb\xe5\x8a\xa1: {node.task_type === 'selection' ? '\xe9\x80\x89\xe8\x82\xa1' : '\xe5\x9b\x9e\xe6\xb5\x8b'}</span>
                      {node.current_task_id and <span>\xe4\xbb\xbb\xe5\x8a\xa1ID: #{node.current_task_id}</span>}
                    </div>
                  )}
                </div>
              ))}
            </div>"""

with open('src/app/page.tsx', 'rb') as f:
    content = f.read()

if old in content:
    content = content.replace(old, new)
    with open('src/app/page.tsx', 'wb') as f:
        f.write(content)
    print('Replaced successfully')
else:
    print('NOT FOUND')
    # Find the section
    idx = content.find(b'clusterStatus')
    if idx >= 0:
        print('Found at:', idx)
        print(content[idx:idx+200])
    else:
        print('Not found')