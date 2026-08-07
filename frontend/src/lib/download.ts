import { parseExportFilename } from './export';

export async function downloadFromResponse(url: string): Promise<void> {
  let res: Response;
  try {
    res = await fetch(url, { cache: 'no-store' });
  } catch {
    alert('导出失败，请重试');
    return;
  }
  if (res.status === 401) {
    window.location.href = '/login';
    return;
  }
  if (res.status === 403) {
    alert('无权限');
    return;
  }
  if (!res.ok) {
    alert('导出失败，请重试');
    return;
  }
  try {
    const blob = await res.blob();
    const filename = parseExportFilename(res.headers.get('Content-Disposition'), 'export.json');
    const objectUrl = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = objectUrl;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(objectUrl);
  } catch {
    alert('导出失败，请重试');
  }
}
