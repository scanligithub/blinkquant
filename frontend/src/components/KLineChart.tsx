'use client';
import { createChart, ColorType, IChartApi, LineData, Time } from 'lightweight-charts';
import { useEffect, useRef, useState, useCallback } from 'react';
import {
  calculateMA,
  calculateEMAForData,
  calculateWMA,
  calculateSMMA,
  calculateBoll,
  calculateVWAP,
  calculateSAR,
  calculateIchimoku,
  calculateBBI,
  calculateMACD,
  calculateRSI,
  calculateKDJ,
  calculateWR,
  calculateOBV,
  calculateCCI,
  calculateDMI,
  calculateMFI,
  calculateVolumeMA,
  calculateRollingSum,
} from '../utils/indicators';

// 内联 Settings 图标组件
const SettingsIcon = ({ className }: { className?: string }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    width="24"
    height="24"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={className}
  >
    <path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z" />
    <circle cx="12" cy="12" r="3" />
  </svg>
);

function formatVolume(volume: number): string {
  if (volume >= 100000000) {
    return (volume / 100000000).toFixed(2) + '亿';
  } else if (volume >= 10000) {
    return (volume / 10000).toFixed(2) + '万';
  }
  return volume.toString();
}

// 【新增】：智能资金单位格式化器
function formatMoney(value: number): string {
  if (!value || isNaN(value)) return '0.00';
  const absVal = Math.abs(value);

  // 假设原始数据单位是 "元"
  if (absVal >= 100000000) {
    return (value / 100000000).toFixed(2) + '亿';
  } else if (absVal >= 10000) {
    return (value / 10000).toFixed(2) + '万';
  }
  return value.toFixed(2);
}

// 主图指标配置
const MAIN_INDICATORS = {
  MA: {
    label: 'MA (简单移动平均)',
    periods: [5, 10, 20, 30, 60, 120],
    colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'],
    calculate: calculateMA,
  },
  EMA: {
    label: 'EMA (指数移动平均)',
    periods: [5, 10, 20, 30, 60, 120],
    colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'],
    calculate: calculateEMAForData,
  },
  WMA: {
    label: 'WMA (加权移动平均)',
    periods: [5, 10, 20, 30, 60, 120],
    colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'],
    calculate: calculateWMA,
  },
  SMMA: {
    label: 'SMMA (平滑移动平均)',
    periods: [5, 10, 20, 30, 60, 120],
    colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'],
    calculate: calculateSMMA,
  },
  BOLL: {
    label: 'BOLL (布林带)',
    periods: [20],
    colors: { upper: '#FF6B6B', middle: '#45B7D1', lower: '#96CEB4' },
    calculate: calculateBoll,
  },
  VWAP: {
    label: 'VWAP (成交量加权平均价)',
    periods: [],
    colors: ['#45B7D1'],
    calculate: calculateVWAP,
  },
  SAR: {
    label: 'SAR (抛物线转向)',
    periods: [],
    colors: ['#FF6B6B'],
    calculate: calculateSAR,
  },
  ICHIMOKU: {
    label: 'Ichimoku (云图)',
    periods: [],
    colors: { tenkan: '#FF6B6B', kijun: '#45B7D1', spanA: '#4ECDC4', spanB: '#96CEB4' },
    calculate: calculateIchimoku,
  },
  BBI: {
    label: 'BBI (多空分界)',
    periods: [],
    colors: ['#45B7D1'],
    calculate: calculateBBI,
  },
  NONE: {
    label: '无指标',
    periods: [],
    colors: [],
    calculate: () => [],
  },
};

// 副图指标配置
const SUB_INDICATORS = {
  MACD: { label: 'MACD (平滑异同移动平均)' },
  MF: { label: '资金流 (主力净流入)' },
  RSI: { label: 'RSI (相对强弱指数)' },
  KDJ: { label: 'KDJ (随机指标)' },
  WR: { label: 'WR (威廉指标)' },
  OBV: { label: 'OBV (累积量)' },
  CCI: { label: 'CCI (顺势指标)' },
  DMI: { label: 'DMI/ADX (趋向指标)' },
  MFI: { label: 'MFI (资金流量指数)' },
  VOL: { label: 'VOL (成交量)' },
};

interface KLineChartProps {
  data: any;
  code: string;
  subChartType?: string;
  onSubChartTypeChange?: (type: string) => void;
  mainChartType?: string;
  onMainChartTypeChange?: (type: string) => void;
}

export default function KLineChart({ 
  data, 
  code, 
  subChartType = 'MACD',
  onSubChartTypeChange,
  mainChartType = 'MA',
  onMainChartTypeChange
}: KLineChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesMap = useRef<{ 
    [key: string]: any 
  }>({});

  const [tooltip, setTooltip] = useState<any>(null);
  const [macdIndicators, setMacdIndicators] = useState<any>(null);
  const [mfIndicators, setMfIndicators] = useState<any>(null);
  const [maIndicators, setMaIndicators] = useState<any>(null);
  const [volumeMaIndicators, setVolumeMaIndicators] = useState<any>(null);
  const [rsiIndicators, setRsiIndicators] = useState<any>(null);
  const [kdjIndicators, setKdjIndicators] = useState<any>(null);
  const [wrIndicators, setWrIndicators] = useState<any>(null);
  const [obvIndicators, setObvIndicators] = useState<any>(null);
  const [cciIndicators, setCciIndicators] = useState<any>(null);
  const [dmiIndicators, setDmiIndicators] = useState<any>(null);
  const [mfiIndicators, setMfiIndicators] = useState<any>(null);
  const [priceExtremes, setPriceExtremes] = useState<any>(null);
  const [volumeMax, setVolumeMax] = useState<any>(null);
  const [extremesPositions, setExtremesPositions] = useState<any>(null);
  const [isMobile, setIsMobile] = useState(false);
  
  // 使用 ref 跟踪 mainChartType 的最新值（用于回调函数中）
  const mainChartTypeRef = useRef(mainChartType);
  useEffect(() => {
    mainChartTypeRef.current = mainChartType;
  }, [mainChartType]);
  
  // 配置菜单状态
  const [mainMenuOpen, setMainMenuOpen] = useState(false);
  const [subMenuOpen, setSubMenuOpen] = useState(false);
  const mainMenuRef = useRef<HTMLDivElement>(null);
  const subMenuRef = useRef<HTMLDivElement>(null);

  // 点击外部关闭菜单
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (mainMenuRef.current && !mainMenuRef.current.contains(event.target as Node)) {
        setMainMenuOpen(false);
      }
      if (subMenuRef.current && !subMenuRef.current.contains(event.target as Node)) {
        setSubMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // 主图指标切换处理
  const handleMainSelect = useCallback((type: string) => {
    setMainMenuOpen(false);
    if (onMainChartTypeChange) {
      onMainChartTypeChange(type);
    }
  }, [onMainChartTypeChange]);

  // 副图指标切换处理
  const handleSubSelect = useCallback((type: string) => {
    setSubMenuOpen(false);
    if (onSubChartTypeChange) {
      onSubChartTypeChange(type);
    }
  }, [onSubChartTypeChange]);

  useEffect(() => {
    if (!chartContainerRef.current || !data) return;

    // 检测是否为移动端
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 768);
    };
    checkMobile();

    const chart = createChart(chartContainerRef.current, {
      layout: { background: { type: ColorType.Solid, color: '#ffffff' }, textColor: '#334155' },
      grid: { vertLines: { color: '#f1f5f9' }, horzLines: { color: '#f1f5f9' } },
      width: chartContainerRef.current.clientWidth,
      height: chartContainerRef.current.clientHeight,
      rightPriceScale: { borderColor: '#e2e8f0' },
      timeScale: { borderColor: '#e2e8f0', timeVisible: true, secondsVisible: false },
      localization: { locale: 'zh-CN', dateFormat: 'yyyy-MM-dd' },
    });

    const candlestickSeries = chart.addCandlestickSeries({
      priceScaleId: 'right', upColor: '#ef4444', downColor: '#22c55e', borderVisible: false, wickUpColor: '#ef4444', wickDownColor: '#22c55e',
    });

    const maPeriods = [5, 10, 20, 30, 60, 120];
    const maColors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'];
    const maSeries = maPeriods.map((period, index) =>
      chart.addLineSeries({ priceScaleId: 'right', color: maColors[index], lineWidth: 1, lastValueVisible: false, priceLineVisible: false })
    );

    const volumeSeries = chart.addHistogramSeries({
      priceScaleId: 'volume', priceFormat: { type: 'volume' }, lastValueVisible: false, priceLineVisible: false,
    });

    const volumeMAPeriods = [5, 10, 20];
    const volumeMAColors = ['#FF6B6B', '#4ECDC4', '#45B7D1'];
    const volumeMASeries = volumeMAPeriods.map((period, index) =>
      chart.addLineSeries({ priceScaleId: 'volume', color: volumeMAColors[index], lineWidth: 1, lastValueVisible: false, priceLineVisible: false })
    );

    // MACD 系列
    const macdLine = chart.addLineSeries({ priceScaleId: 'subchart', color: '#ef4444', lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
    const signalLine = chart.addLineSeries({ priceScaleId: 'subchart', color: '#22c55e', lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
    const histogramSeries = chart.addHistogramSeries({ priceScaleId: 'subchart', baseLineColor: '#e2e8f0', lastValueVisible: false, priceLineVisible: false });

    // 资金流柱状图
    const mfSeries = chart.addHistogramSeries({
      priceScaleId: 'subchart',
      baseLineColor: '#e2e8f0',
      baseLineVisible: true,
      priceFormat: {
        type: 'custom',
        formatter: (price: number) => formatMoney(price)
      },
      lastValueVisible: false, priceLineVisible: false,
    });

    // 资金流 20 日累计趋势折线 (黄色)
    const mfTrendLine = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#f59e0b',
      lineWidth: 2,
      lastValueVisible: false, priceLineVisible: false,
    });

    // RSI 系列
    const rsiSeries = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#9b59b6',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });
  
    // KDJ 系列 (K, D, J 三条线)
    const kdjKSeries = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#e74c3c',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });
    const kdjDSeries = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#3498db',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });
    const kdjJSeries = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#f1c40f',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });
  
    // WR 系列 (威廉指标)
    const wrSeries = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#e67e22',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });
  
    // OBV 系列 (能量潮)
    const obvSeries = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#1abc9c',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });
  
    // CCI 系列 (顺势指标)
    const cciSeries = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#34495e',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });
  
    // DMI 系列 (+DI, -DI, ADX 三条线)
    const dmiPdiSeries = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#e74c3c',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });
    const dmiMdiSeries = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#3498db',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });
    const dmiAdxSeries = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#9b59b6',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });
  
    // MFI 系列 (资金流量指数)
    const mfiSeries = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#e91e63',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });
  
    seriesMap.current = {
      macdLine,
      signalLine,
      histogramSeries,
      mfSeries,
      mfTrendLine,
      rsiSeries,
      kdjKSeries,
      kdjDSeries,
      kdjJSeries,
      wrSeries,
      obvSeries,
      cciSeries,
      dmiPdiSeries,
      dmiMdiSeries,
      dmiAdxSeries,
      mfiSeries,
      maSeries,
      volumeMASeries,
    };

    chart.priceScale('right').applyOptions({ scaleMargins: { top: 0.05, bottom: 0.40 } });
    chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.60, bottom: 0.25 } });
    chart.priceScale('subchart').applyOptions({ scaleMargins: { top: 0.75, bottom: 0.0 } });

    const formattedData = data.map((item: any) => ({ time: item.time, open: item.open, high: item.high, low: item.low, close: item.close }));
    const volumeData = data.map((item: any) => ({ time: item.time, value: item.volume, color: item.close >= item.open ? '#ef4444' : '#22c55e' }));

    // 主图极值和量能最大值将在可视范围内动态计算
    setPriceExtremes(null);
    setVolumeMax(null);

    const mfData = data.map((item: any) => {
      const val = item.main_net || 0;
      return { time: item.time, value: val, color: val >= 0 ? 'rgba(239, 68, 68, 0.85)' : 'rgba(34, 197, 94, 0.85)' };
    });

    candlestickSeries.setData(formattedData);
    volumeSeries.setData(volumeData);
    maPeriods.forEach((period, index) => maSeries[index].setData(calculateMA(formattedData, period)));

    // 计算极值在图表中的位置（基于可视范围）
    const calculatePositions = () => {
      if (!chartContainerRef.current) return;

      const positions: any = {};
      const timeScale = chart.timeScale();
      const visibleRange = timeScale.getVisibleRange();

      if (visibleRange) {
        const { from, to } = visibleRange;

        // 主图最高价位置（基于可视范围）
        let maxPrice = -Infinity;
        let maxPriceTime: any = null;

        // 主图最低价位置（基于可视范围）
        let minPrice = Infinity;
        let minPriceTime: any = null;

        // 量能最大值位置（基于可视范围）
        let maxVol = 0;
        let maxVolTime: any = null;

        formattedData.forEach(d => {
          const timeValue = typeof d.time === 'number' ? d.time : (d.time as any).businessDay || d.time;
          if (timeValue >= from && timeValue <= to) {
            if (d.high > maxPrice) {
              maxPrice = d.high;
              maxPriceTime = d.time;
            }
            if (d.low < minPrice) {
              minPrice = d.low;
              minPriceTime = d.time;
            }
          }
        });

        volumeData.forEach(d => {
          const timeValue = typeof d.time === 'number' ? d.time : (d.time as any).businessDay || d.time;
          if (timeValue >= from && timeValue <= to) {
            if (d.value > maxVol) {
              maxVol = d.value;
              maxVolTime = d.time;
            }
          }
        });

        // 主图最高价位置
        if (maxPriceTime !== null) {
          const x = timeScale.timeToCoordinate(maxPriceTime);
          const y = candlestickSeries.priceToCoordinate(maxPrice);
          if (x !== null && y !== null) {
            positions.maxPrice = { x, y, value: maxPrice, label: '高' };
          }
        }

        // 主图最低价位置
        if (minPriceTime !== null) {
          const x = timeScale.timeToCoordinate(minPriceTime);
          const y = candlestickSeries.priceToCoordinate(minPrice);
          if (x !== null && y !== null) {
            positions.minPrice = { x, y, value: minPrice, label: '低' };
          }
        }

        // 量能最大值位置
        if (maxVolTime !== null) {
          const x = timeScale.timeToCoordinate(maxVolTime);
          const y = volumeSeries.priceToCoordinate(maxVol);
          if (x !== null && y !== null) {
            positions.maxVolume = { x, y, value: maxVol, label: '最大' };
          }
        }
      }

      setExtremesPositions(positions);
    };

    // 初始计算位置
    setTimeout(calculatePositions, 100);

    // 监听图表尺寸变化和可见范围变化
    const handleVisibleRangeChange = () => {
      calculatePositions();
    };
    chart.timeScale().subscribeVisibleLogicalRangeChange(handleVisibleRangeChange);
    chart.timeScale().subscribeVisibleTimeRangeChange(handleVisibleRangeChange);
    volumeMAPeriods.forEach((period, index) => volumeMASeries[index].setData(calculateVolumeMA(volumeData, period)));

    const macdData = calculateMACD(formattedData);
    macdLine.setData(macdData.macdLine);
    signalLine.setData(macdData.signalLine);
    histogramSeries.setData(macdData.histogram);

    mfSeries.setData(mfData);
    // 渲染 20 日累计趋势线
    mfTrendLine.setData(calculateRollingSum(mfData, 20));

    // 根据初始 subChartType 设置副图系列的可见性
    const isMacd = subChartType === 'MACD';
    macdLine.applyOptions({ visible: isMacd });
    signalLine.applyOptions({ visible: isMacd });
    histogramSeries.applyOptions({ visible: isMacd });
    mfSeries.applyOptions({ visible: !isMacd });
    mfTrendLine.applyOptions({ visible: !isMacd });

    // 根据屏幕宽度设置 Y 轴可见性
    const updateYAxisVisibility = () => {
      const mobile = window.innerWidth < 768;
      setIsMobile(mobile);
      if (chartRef.current) {
        chartRef.current.priceScale('right').applyOptions({ visible: !mobile });
        chartRef.current.priceScale('volume').applyOptions({ visible: !mobile });
        chartRef.current.priceScale('subchart').applyOptions({ visible: !mobile });
      }
    };
    
    // 初始设置 Y 轴可见性
    updateYAxisVisibility();
    
    // 监听窗口大小变化
    const handleResize = () => {
      updateYAxisVisibility();
      calculatePositions();
    };
    window.addEventListener('resize', handleResize);

    chart.subscribeCrosshairMove((param) => {
      if (!param.point || !param.time || !param.seriesData.size) { setTooltip(null); return; }
      const cdData = param.seriesData.get(candlestickSeries);
      const volData = param.seriesData.get(volumeSeries);

      if (cdData && typeof cdData === 'object' && 'open' in cdData) {
        const timeValue = typeof param.time === 'number' ? param.time : (param.time as any).businessDay || param.time;
        const date = new Date(timeValue * 1000);

        setTooltip({
          time: date.toLocaleDateString('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit' }),
          open: cdData.open as number, high: cdData.high as number, low: cdData.low as number, close: cdData.close as number,
          volume: (volData as any)?.value || 0,
          changePercent: (((cdData.close as number) - (cdData.open as number)) / (cdData.open as number)) * 100,
          position: param.point.x < (chartContainerRef.current?.clientWidth || 0) / 2 ? 'right' : 'left',
        });

        setMacdIndicators({
          dif: (param.seriesData.get(macdLine) as any)?.value || 0,
          dea: (param.seriesData.get(signalLine) as any)?.value || 0,
          macd: (param.seriesData.get(histogramSeries) as any)?.value || 0,
        });

        setMfIndicators({
          net: (param.seriesData.get(mfSeries) as any)?.value || 0,
          trend: (param.seriesData.get(mfTrendLine) as any)?.value || 0,
        });

        // RSI 指标
        const rsiValue = (param.seriesData.get(seriesMap.current.rsiSeries) as any)?.value;
        setRsiIndicators(rsiValue !== undefined && rsiValue !== 0 ? { rsi: rsiValue } : null);

        // KDJ 指标
        const kdjK = (param.seriesData.get(seriesMap.current.kdjKSeries) as any)?.value;
        const kdjD = (param.seriesData.get(seriesMap.current.kdjDSeries) as any)?.value;
        const kdjJ = (param.seriesData.get(seriesMap.current.kdjJSeries) as any)?.value;
        if (kdjK !== undefined && kdjK !== 0) {
          setKdjIndicators({ k: kdjK, d: kdjD, j: kdjJ });
        } else {
          setKdjIndicators(null);
        }

        // WR 指标
        const wrValue = (param.seriesData.get(seriesMap.current.wrSeries) as any)?.value;
        setWrIndicators(wrValue !== undefined && wrValue !== 0 ? { wr: wrValue } : null);

        // OBV 指标
        const obvValue = (param.seriesData.get(seriesMap.current.obvSeries) as any)?.value;
        setObvIndicators(obvValue !== undefined && obvValue !== 0 ? { obv: obvValue } : null);

        // CCI 指标
        const cciValue = (param.seriesData.get(seriesMap.current.cciSeries) as any)?.value;
        setCciIndicators(cciValue !== undefined && cciValue !== 0 ? { cci: cciValue } : null);

        // DMI 指标
        const dmiPdi = (param.seriesData.get(seriesMap.current.dmiPdiSeries) as any)?.value;
        const dmiMdi = (param.seriesData.get(seriesMap.current.dmiMdiSeries) as any)?.value;
        const dmiAdx = (param.seriesData.get(seriesMap.current.dmiAdxSeries) as any)?.value;
        if (dmiPdi !== undefined && dmiPdi !== 0) {
          setDmiIndicators({ pdi: dmiPdi, mdi: dmiMdi, adx: dmiAdx });
        } else {
          setDmiIndicators(null);
        }

        // MFI 指标
        const mfiValue = (param.seriesData.get(seriesMap.current.mfiSeries) as any)?.value;
        setMfiIndicators(mfiValue !== undefined && mfiValue !== 0 ? { mfi: mfiValue } : null);

        // 获取主图指标最新值（根据当前选择的指标类型显示对应名称）
        const mainChartValues: any = {};
        maSeries.forEach((series, index) => {
          const value = (param.seriesData.get(series) as any)?.value;
          if (value !== undefined && value !== 0) {
            // 根据当前主图指标类型生成对应的标签（使用 ref 获取最新值）
            let label: string;
            const currentType = mainChartTypeRef.current || 'MA';
            if (currentType === 'MA' || currentType === 'EMA') {
              label = `${currentType}${maPeriods[index]}`;
            } else if (currentType === 'BOLL') {
              const bollLabels = ['中轨', '上轨', '下轨'];
              label = bollLabels[index] || `BOLL${index}`;
            } else {
              label = `Line${index}`;
            }
            mainChartValues[label] = { value, color: maColors[index] };
          }
        });
        setMaIndicators(mainChartValues);

        // 获取量能 MA 最新值
        const volumeMaValues: any = {};
        volumeMASeries.forEach((series, index) => {
          const value = (param.seriesData.get(series) as any)?.value;
          if (value !== undefined) {
            volumeMaValues[`VMA${volumeMAPeriods[index]}`] = { value, color: volumeMAColors[index] };
          }
        });
        setVolumeMaIndicators(volumeMaValues);
      }
    });

    chart.timeScale().fitContent();
    chartRef.current = chart;

    const resizeObserver = new ResizeObserver(() => {
      if (chartContainerRef.current) chart.applyOptions({ width: chartContainerRef.current.clientWidth, height: chartContainerRef.current.clientHeight });
    });
    resizeObserver.observe(chartContainerRef.current);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
      window.removeEventListener('resize', handleResize);
    };
  }, [data]);

  // 响应 subChartType 变化 - 更新副图指标显示
  useEffect(() => {
    if (!seriesMap.current.mfSeries || !data) return;
  
    const formattedData = data.map((item: any) => ({ time: item.time, open: item.open, high: item.high, low: item.low, close: item.close }));
    const volumeData = data.map((item: any) => ({ time: item.time, value: item.volume, color: item.close >= item.open ? '#ef4444' : '#22c55e' }));
    const mfData = data.map((item: any) => {
      const val = item.main_net || 0;
      return { time: item.time, value: val, color: val >= 0 ? 'rgba(239, 68, 68, 0.85)' : 'rgba(34, 197, 94, 0.85)' };
    });
  
    // 默认隐藏所有副图系列
    seriesMap.current.macdLine.applyOptions({ visible: false });
    seriesMap.current.signalLine.applyOptions({ visible: false });
    seriesMap.current.histogramSeries.applyOptions({ visible: false });
    seriesMap.current.mfSeries.applyOptions({ visible: false });
    seriesMap.current.mfTrendLine.applyOptions({ visible: false });
  
    if (subChartType === 'MACD') {
      const macdData = calculateMACD(formattedData);
      seriesMap.current.macdLine.setData(macdData.macdLine);
      seriesMap.current.signalLine.setData(macdData.signalLine);
      seriesMap.current.histogramSeries.setData(macdData.histogram);
      seriesMap.current.macdLine.applyOptions({ visible: true });
      seriesMap.current.signalLine.applyOptions({ visible: true });
      seriesMap.current.histogramSeries.applyOptions({ visible: true });
    } else if (subChartType === 'MF') {
      seriesMap.current.mfSeries.setData(mfData);
      seriesMap.current.mfTrendLine.setData(calculateRollingSum(mfData, 20));
      seriesMap.current.mfSeries.applyOptions({ visible: true });
      seriesMap.current.mfTrendLine.applyOptions({ visible: true });
    } else if (subChartType === 'RSI') {
      const rsiData = calculateRSI(formattedData, 14);
      if (seriesMap.current.rsiSeries) {
        seriesMap.current.rsiSeries.setData(rsiData);
        seriesMap.current.rsiSeries.applyOptions({ visible: true });
      }
    } else if (subChartType === 'KDJ') {
      const kdjData = calculateKDJ(formattedData, 9, 3, 3);
      if (seriesMap.current.kdjKSeries) {
        seriesMap.current.kdjKSeries.setData(kdjData.k);
        seriesMap.current.kdjDSeries.setData(kdjData.d);
        seriesMap.current.kdjJSeries.setData(kdjData.j);
        seriesMap.current.kdjKSeries.applyOptions({ visible: true });
        seriesMap.current.kdjDSeries.applyOptions({ visible: true });
        seriesMap.current.kdjJSeries.applyOptions({ visible: true });
      }
    } else if (subChartType === 'WR') {
      const wrData = calculateWR(formattedData, 14);
      if (seriesMap.current.wrSeries) {
        seriesMap.current.wrSeries.setData(wrData);
        seriesMap.current.wrSeries.applyOptions({ visible: true });
      }
    } else if (subChartType === 'OBV') {
      const obvData = calculateOBV(formattedData);
      if (seriesMap.current.obvSeries) {
        seriesMap.current.obvSeries.setData(obvData);
        seriesMap.current.obvSeries.applyOptions({ visible: true });
      }
    } else if (subChartType === 'CCI') {
      const cciData = calculateCCI(formattedData, 20);
      if (seriesMap.current.cciSeries) {
        seriesMap.current.cciSeries.setData(cciData);
        seriesMap.current.cciSeries.applyOptions({ visible: true });
      }
    } else if (subChartType === 'DMI') {
      const dmiData = calculateDMI(formattedData, 14);
      if (seriesMap.current.dmiPdiSeries) {
        seriesMap.current.dmiPdiSeries.setData(dmiData.pdi);
        seriesMap.current.dmiMdiSeries.setData(dmiData.mdi);
        seriesMap.current.dmiAdxSeries.setData(dmiData.adx);
        seriesMap.current.dmiPdiSeries.applyOptions({ visible: true });
        seriesMap.current.dmiMdiSeries.applyOptions({ visible: true });
        seriesMap.current.dmiAdxSeries.applyOptions({ visible: true });
      }
    } else if (subChartType === 'MFI') {
      const mfiData = calculateMFI(formattedData, 14);
      if (seriesMap.current.mfiSeries) {
        seriesMap.current.mfiSeries.setData(mfiData);
        seriesMap.current.mfiSeries.applyOptions({ visible: true });
      }
    } else if (subChartType === 'VOL') {
      // VOL 已经在主图表中显示，这里可以显示成交量均线
      [5, 10, 20].forEach((period, index) => {
        if (seriesMap.current.volumeMASeries && seriesMap.current.volumeMASeries[index]) {
          seriesMap.current.volumeMASeries[index].setData(calculateVolumeMA(volumeData, period));
          seriesMap.current.volumeMASeries[index].applyOptions({ visible: true });
        }
      });
    }
  }, [subChartType, data]);

  // 响应 mainChartType 变化 - 重新计算并设置主图指标
  useEffect(() => {
    if (!seriesMap.current.maSeries || !chartRef.current || !data) return;
  
    const formattedData = data.map((item: any) => ({ time: item.time, open: item.open, high: item.high, low: item.low, close: item.close }));
    const maSeries = seriesMap.current.maSeries;
  
    // 清除旧指标数据
    maSeries.forEach((series: any) => series.setData([]));
  
    if (mainChartType === 'MA' || mainChartType === 'EMA' || mainChartType === 'WMA' || mainChartType === 'SMMA') {
      const config = MAIN_INDICATORS[mainChartType];
      config.periods.forEach((period, index) => {
        if (maSeries[index]) {
          const dataPoints = config.calculate(formattedData, period);
          maSeries[index].setData(dataPoints);
        }
      });
    } else if (mainChartType === 'BOLL') {
      const bollData = calculateBoll(formattedData, 20);
      if (maSeries[0]) maSeries[0].setData(bollData.middle);
      if (maSeries[1]) maSeries[1].setData(bollData.upper);
      if (maSeries[2]) maSeries[2].setData(bollData.lower);
      for (let i = 3; i < maSeries.length; i++) {
        maSeries[i].setData([]);
      }
    } else if (mainChartType === 'VWAP') {
      const vwapData = calculateVWAP(formattedData);
      if (maSeries[0]) maSeries[0].setData(vwapData);
      for (let i = 1; i < maSeries.length; i++) {
        maSeries[i].setData([]);
      }
    } else if (mainChartType === 'SAR') {
      const sarData = calculateSAR(formattedData);
      if (maSeries[0]) maSeries[0].setData(sarData);
      for (let i = 1; i < maSeries.length; i++) {
        maSeries[i].setData([]);
      }
    } else if (mainChartType === 'ICHIMOKU') {
      const ichimokuData = calculateIchimoku(formattedData);
      if (maSeries[0]) maSeries[0].setData(ichimokuData.tenkan);
      if (maSeries[1]) maSeries[1].setData(ichimokuData.kijun);
      if (maSeries[2]) maSeries[2].setData(ichimokuData.spanA);
      if (maSeries[3]) maSeries[3].setData(ichimokuData.spanB);
      for (let i = 4; i < maSeries.length; i++) {
        maSeries[i].setData([]);
      }
    } else if (mainChartType === 'BBI') {
      const bbiData = calculateBBI(formattedData);
      if (maSeries[0]) maSeries[0].setData(bbiData);
      for (let i = 1; i < maSeries.length; i++) {
        maSeries[i].setData([]);
      }
    } else if (mainChartType === 'NONE') {
      maSeries.forEach((series: any) => series.setData([]));
    }
  }, [mainChartType, data]);

  return (
    <div className="w-full h-full relative bg-white">
      <div ref={chartContainerRef} className="absolute inset-0 w-full h-full" />

      {/* 主图指标配置按钮和数据显示 - 左上角 */}
      <div ref={mainMenuRef} className="absolute top-2 left-2 md:left-4 z-20 flex items-center gap-2">
        <div className="relative">
          <button
            onClick={() => setMainMenuOpen(!mainMenuOpen)}
            className="p-1.5 rounded-md hover:bg-slate-100 transition-colors bg-white/90 backdrop-blur shadow-sm"
          >
            <SettingsIcon className="w-4 h-4 text-slate-500" />
          </button>
      
          {mainMenuOpen && (
            <div className="absolute left-0 top-full mt-1 bg-white rounded-lg shadow-lg border border-slate-200 py-1 min-w-[100px]">
              {Object.entries(MAIN_INDICATORS).map(([key, config]) => (
                <button
                  key={key}
                  onClick={() => handleMainSelect(key)}
                  className={`w-full px-3 py-1.5 text-left text-sm hover:bg-slate-50 transition-colors ${
                    mainChartType === key ? 'text-blue-600 font-medium bg-blue-50' : 'text-slate-600'
                  }`}
                >
                  {config.label}
                </button>
              ))}
            </div>
          )}
        </div>
      </div>
      
      {/* 主图 MA 指标动态显示 - 紧跟配置按钮 */}
      <div className="absolute top-2 left-2 md:left-4 z-10 bg-transparent px-2 py-1 rounded-lg text-[9px] md:text-xs pointer-events-none" style={{ marginLeft: '48px' }}>
        {maIndicators && Object.entries(maIndicators).map(([key, item]: [string, any]) => (
          <span key={key} className="text-slate-500 mr-2">
            {key}: <span className="font-mono" style={{ color: item.color }}>{item.value.toFixed(2)}</span>
          </span>
        ))}
      </div>
      
      {/* 副图指标配置按钮 - 副图区域左上角 */}
      <div ref={subMenuRef} className="absolute top-[calc(75%-1.5rem)] left-2 md:left-4 z-20">
        <div className="relative">
          <button
            onClick={() => setSubMenuOpen(!subMenuOpen)}
            className="p-1.5 rounded-md hover:bg-slate-100 transition-colors bg-white/90 backdrop-blur shadow-sm"
          >
            <SettingsIcon className="w-4 h-4 text-slate-500" />
          </button>
      
          {subMenuOpen && (
            <div className="absolute left-0 top-full mt-1 bg-white rounded-lg shadow-lg border border-slate-200 py-1 min-w-[100px]">
              {Object.entries(SUB_INDICATORS).map(([key, config]) => (
                <button
                  key={key}
                  onClick={() => handleSubSelect(key)}
                  className={`w-full px-3 py-1.5 text-left text-sm hover:bg-slate-50 transition-colors ${
                    subChartType === key ? 'text-blue-600 font-medium bg-blue-50' : 'text-slate-600'
                  }`}
                >
                  {config.label}
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* 主图极值显示 */}
      {extremesPositions && (
        <>
          {extremesPositions.maxPrice && (
            <div
              className="absolute z-10 bg-white/95 backdrop-blur px-2 py-1 rounded border border-red-200 shadow-sm text-[9px] md:text-xs pointer-events-none"
              style={{
                left: `${extremesPositions.maxPrice.x}px`,
                top: `${extremesPositions.maxPrice.y - 30}px`,
                transform: 'translateX(-50%)'
              }}
            >
              <div className="flex items-center gap-1">
                <span className="text-red-600 font-bold">{extremesPositions.maxPrice.label}</span>
                <span className="font-mono text-red-600">{extremesPositions.maxPrice.value.toFixed(2)}</span>
              </div>
              <div className="absolute left-1/2 -bottom-2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-red-200 transform -translate-x-1/2"></div>
            </div>
          )}
          {extremesPositions.minPrice && (
            <div
              className="absolute z-10 bg-white/95 backdrop-blur px-2 py-1 rounded border border-green-200 shadow-sm text-[9px] md:text-xs pointer-events-none"
              style={{
                left: `${extremesPositions.minPrice.x}px`,
                top: `${extremesPositions.minPrice.y + 10}px`,
                transform: 'translateX(-50%)'
              }}
            >
              <div className="flex items-center gap-1">
                <span className="text-green-600 font-bold">{extremesPositions.minPrice.label}</span>
                <span className="font-mono text-green-600">{extremesPositions.minPrice.value.toFixed(2)}</span>
              </div>
              <div className="absolute left-1/2 -top-2 w-0 h-0 border-l-4 border-r-4 border-b-4 border-transparent border-b-green-200 transform -translate-x-1/2"></div>
            </div>
          )}
        </>
      )}


      {/* 量能极值显示 */}
      {extremesPositions && extremesPositions.maxVolume && (
        <div
          className="absolute z-10 bg-white/95 backdrop-blur px-2 py-1 rounded border border-slate-200 shadow-sm text-[9px] md:text-xs pointer-events-none"
          style={{
            left: `${extremesPositions.maxVolume.x}px`,
            top: `${extremesPositions.maxVolume.y - 30}px`,
            transform: 'translateX(-50%)'
          }}
        >
          <div className="flex items-center gap-1">
            <span className="text-slate-600 font-bold">{extremesPositions.maxVolume.label}</span>
            <span className="font-mono text-slate-900">{formatVolume(extremesPositions.maxVolume.value)}</span>
          </div>
          <div className="absolute left-1/2 -bottom-2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-slate-300 transform -translate-x-1/2"></div>
        </div>
      )}

      {/* 量能 MA 指标动态显示 */}
      <div className="absolute top-[calc(60%-1.2rem)] left-2 md:left-4 z-10 bg-transparent px-2 py-1 rounded-lg text-[9px] md:text-xs pointer-events-none">
        {volumeMaIndicators && Object.entries(volumeMaIndicators).map(([key, item]: [string, any]) => (
          <span key={key} className="text-slate-500 mr-2">
            {key}: <span className="font-mono" style={{ color: item.color }}>{formatVolume(item.value)}</span>
          </span>
        ))}
      </div>

      {/* 副图指标动态显示 - 紧跟副图配置按钮 */}
              <div className="absolute top-[calc(75%-1.2rem)] left-2 md:left-4 z-10 bg-transparent px-2 py-1 rounded-lg text-[9px] md:text-xs pointer-events-none" style={{ marginLeft: '48px' }}>
                {/* MACD */}
                {subChartType === 'MACD' && macdIndicators && (
                  <div className="flex items-center gap-1.5 md:gap-3">
                    <span className="text-slate-500">DIF: <span className="font-mono text-[#ef4444]">{macdIndicators.dif.toFixed(2)}</span></span>
                    <span className="text-slate-500">DEA: <span className="font-mono text-[#22c55e]">{macdIndicators.dea.toFixed(2)}</span></span>
                    <span className="text-slate-500">MACD: <span className={`font-mono ${macdIndicators.macd >= 0 ? 'text-[#ef4444]' : 'text-[#22c55e]'}`}>{macdIndicators.macd.toFixed(2)}</span></span>
                  </div>
                )}
                {/* 资金流 MF */}
                {subChartType === 'MF' && mfIndicators && (
                  <div className="flex items-center gap-1.5 md:gap-3">
                    <span className="text-slate-500">
                      单日主力: <span className={`font-mono ${mfIndicators.net >= 0 ? 'text-[#ef4444]' : 'text-[#22c55e]'}`}>
                        {formatMoney(mfIndicators.net)}
                      </span>
                    </span>
                    <span className="text-slate-500">
                      20 日趋势: <span className="font-mono text-[#f59e0b]">
                        {formatMoney(mfIndicators.trend)}
                      </span>
                    </span>
                  </div>
                )}
                {/* RSI */}
                {subChartType === 'RSI' && rsiIndicators && (
                  <div className="flex items-center gap-1.5 md:gap-3">
                    <span className="text-slate-500">RSI(14): <span className="font-mono text-[#9b59b6]">{rsiIndicators.rsi.toFixed(2)}</span></span>
                  </div>
                )}
                {/* KDJ */}
                {subChartType === 'KDJ' && kdjIndicators && (
                  <div className="flex items-center gap-1.5 md:gap-3">
                    <span className="text-slate-500">K: <span className="font-mono text-[#e74c3c]">{kdjIndicators.k.toFixed(2)}</span></span>
                    <span className="text-slate-500">D: <span className="font-mono text-[#3498db]">{kdjIndicators.d.toFixed(2)}</span></span>
                    <span className="text-slate-500">J: <span className="font-mono text-[#f1c40f]">{kdjIndicators.j.toFixed(2)}</span></span>
                  </div>
                )}
                {/* WR */}
                {subChartType === 'WR' && wrIndicators && (
                  <div className="flex items-center gap-1.5 md:gap-3">
                    <span className="text-slate-500">WR(14): <span className="font-mono text-[#e67e22]">{wrIndicators.wr.toFixed(2)}</span></span>
                  </div>
                )}
                {/* OBV */}
                {subChartType === 'OBV' && obvIndicators && (
                  <div className="flex items-center gap-1.5 md:gap-3">
                    <span className="text-slate-500">OBV: <span className="font-mono text-[#1abc9c]">{formatVolume(obvIndicators.obv)}</span></span>
                  </div>
                )}
                {/* CCI */}
                {subChartType === 'CCI' && cciIndicators && (
                  <div className="flex items-center gap-1.5 md:gap-3">
                    <span className="text-slate-500">CCI(20): <span className="font-mono text-[#34495e]">{cciIndicators.cci.toFixed(2)}</span></span>
                  </div>
                )}
                {/* DMI */}
                {subChartType === 'DMI' && dmiIndicators && (
                  <div className="flex items-center gap-1.5 md:gap-3">
                    <span className="text-slate-500">+DI: <span className="font-mono text-[#e74c3c]">{dmiIndicators.pdi.toFixed(2)}</span></span>
                    <span className="text-slate-500">-DI: <span className="font-mono text-[#3498db]">{dmiIndicators.mdi.toFixed(2)}</span></span>
                    <span className="text-slate-500">ADX: <span className="font-mono text-[#9b59b6]">{dmiIndicators.adx.toFixed(2)}</span></span>
                  </div>
                )}
                {/* MFI */}
                {subChartType === 'MFI' && mfiIndicators && (
                  <div className="flex items-center gap-1.5 md:gap-3">
                    <span className="text-slate-500">MFI(14): <span className="font-mono text-[#e91e63]">{mfiIndicators.mfi.toFixed(2)}</span></span>
                  </div>
                )}
                {/* VOL */}
                {subChartType === 'VOL' && volumeMaIndicators && (
                  <div className="flex items-center gap-1.5 md:gap-3">
                    {Object.entries(volumeMaIndicators).map(([key, item]: [string, any]) => (
                      <span key={key} className="text-slate-500">
                        {key}: <span className="font-mono" style={{ color: item.color }}>{formatVolume(item.value)}</span>
                      </span>
                    ))}
                  </div>
                )}
              </div>

      {/* 主图悬浮框 */}
      {tooltip && (
        <div className={`absolute top-8 z-20 bg-white/95 backdrop-blur px-3 py-2 rounded-lg border border-slate-200 shadow-lg text-[10px] md:text-xs pointer-events-none ${
          tooltip.position === 'left' ? 'left-4' : 'right-12'
        }`}>
          <div className="font-bold text-slate-900 mb-1">{tooltip.time}</div>
          <div className="space-y-0.5">
            <div className="flex justify-between gap-4"><span className="text-slate-500">开盘:</span><span className="font-mono text-slate-900">{tooltip.open.toFixed(2)}</span></div>
            <div className="flex justify-between gap-4"><span className="text-slate-500">收盘:</span><span className="font-mono text-slate-900">{tooltip.close.toFixed(2)}</span></div>
            <div className="flex justify-between gap-4"><span className="text-slate-500">成交量:</span><span className="font-mono text-slate-900">{formatVolume(tooltip.volume)}</span></div>
            <div className="flex justify-between gap-4"><span className="text-slate-500">涨幅:</span><span className={`font-mono ${tooltip.changePercent >= 0 ? 'text-red-600' : 'text-green-600'}`}>{tooltip.changePercent >= 0 ? '+' : ''}{tooltip.changePercent.toFixed(2)}%</span></div>
          </div>
        </div>
      )}
    </div>
  );
}
