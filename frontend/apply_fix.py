# -*- coding: utf-8 -*-
with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

# The old section to replace (from line 682 to 722)
old = '''          <div className="flex flex-col gap-3">
            <div className="flex justify-center">
              <div className="text-xs md:text-sm font-mono text-slate-400 bg-white px-3 py-2 rounded-lg border shadow-sm">
                集群: {clusterStatus?.cluster_health || '连接中...'}
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
                        <span className="text-slate-500 text-xs md:text-sm">进程内存</span>
                        <div className="font-mono font-medium text-slate-900 text-right text-xs md:text-sm">{node.process_memory_gb} GB</div>
                      </div>
                      <div className="flex justify-between gap-3">
                        <span className="text-slate-500 text-xs md:text-sm">系统空闲</span>
                        <div className="font-mono font-bold text-blue-600 text-right text-xs md:text-sm">{node.system_memory_free_gb} GB</div>
                      </div>
                      <div className="flex justify-between gap-3">
                        <span className="text-slate-500 text-xs md:text-sm">磁盘空闲</span>
                        <div className="font-mono font-bold text-slate-900 text-right text-xs md:text-sm">{node.disk_free_gb} GB</div>
                      </div>
                      <div className="flex justify-between gap-3">
                        <span className="text-slate-500 text-xs md:text-sm">数据行</span>
                        <div className="font-mono text-slate-500 text-right text-xs md:text-sm">{node.rows_daily?.toLocaleString()}</div>
                      </div>
                    </div>
                  ) : null}
                </div>
              ))}
            </div>'''

new = '''          <div className="flex flex-col gap-3">
            <div className="flex justify-center">
              <div className="text-xs md:text-sm font-mono text-slate-400 bg-white px-3 py-2 rounded-lg border shadow-sm">
                集群: {clusterStatus?.queueStats ? (clusterStatus.queueStats.running > 0 ? '运行中' : '空闲') : '连接中...'}
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
                      {node.status === 'idle' ? '空闲' : node.status === 'running' ? '运行中' : node.status}
                    </span>
                  </div>
                  {node.status === 'running' && node.task_type && (
                    <div className="mt-2 space-y-1 text-xs">
                      <span>任务: {node.task_type === 'selection' ? '选股' : '回测'}</span>
                      {node.current_task_id && <span>任务ID: #{node.current_task_id}</span>}
                    </div>
                  )}
                </div>
              ))}
            </div>'''

with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

if old in content:
    content = content.replace(old, new)
    with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'w', encoding='utf-8') as f:
        f.write(content)
    print('Replaced successfully')
else:
    print('NOT FOUND - searching for partial match')
    # Find the position of key phrases
    idx = content.find('集群: {clusterStatus')
    if idx >= 0:
        print('Found clusterStatus at:', idx)
        print(repr(content[idx:idx+500]))
    else:
        print('clusterStatus not found')
        
    idx2 = content.find('node.online ?')
    if idx2 >= 0:
        print('Found node.online at:', idx2)
        print(repr(content[idx2:idx2+500]))