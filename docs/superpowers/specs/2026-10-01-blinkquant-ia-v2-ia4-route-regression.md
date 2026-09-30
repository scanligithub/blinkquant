# BlinkQuant IA v2 — IA4 路由回归与旧页面清理

状态：实施中

## 范围

- 对照 IA v2 页面树，检查所有一级页面与核心详情页路由文件。
- 锁定 `/` 和 `/select` 继续复用同一个 `SelectionWorkspace`，不改变登录后的默认选股工作流。
- 检查八个一级导航项都已启用。
- 清理已被 `SelectionWorkspace` 替代的历史聚合页面备份 `frontend/src/app/page.tsx.working`；它不是 Next.js 路由，也没有代码引用。
- 将上述静态回归纳入 Frontend CI，并继续使用 Next.js production build 验证路由编译。

## 验收边界

自动化检查只能证明路由文件、默认入口和导航配置满足静态契约；不能替代生产登录、权限隔离、移动端布局、真实任务执行或任务详情跳转的浏览器端 E2E。那些依赖后端状态的验收继续单独执行。

## 不在本阶段改动

- Scheduler / 选股优先与回测抢占逻辑。
- API 合约、用户数据 schema、Artifact 存储。
- 现有各页面业务状态与计算内核。
