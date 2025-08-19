<script setup lang="ts">
import type { Group, PoolStatsResponse } from "@/types/models";
import {
    AlertCircleOutline,
    BarChartOutline,
    SettingsOutline,
    SpeedometerOutline,
    TrendingUpOutline
} from "@vicons/ionicons5";
import {
    NButton,
    NCard,
    NGrid,
    NGridItem,
    NIcon,
    NProgress,
    NSpace,
    NTooltip,
    useMessage,
} from "naive-ui";
import { computed } from "vue";

interface Props {
  group: Group | null;
  poolStats: PoolStatsResponse | null;
}

interface Emits {
  (e: "operation", operation: string, data?: any): void;
}

const props = defineProps<Props>();
const emit = defineEmits<Emits>();

// 计算性能指标颜色
const performanceColors = computed(() => {
  if (!props.poolStats?.performance_metrics) {
    return {
      errorRate: "default",
      cacheHitRate: "default",
    };
  }
  
  const metrics = props.poolStats.performance_metrics;
  return {
    errorRate: metrics.error_rate < 0.05 ? "success" : metrics.error_rate < 0.1 ? "warning" : "error",
    cacheHitRate: metrics.cache_hit_rate > 0.8 ? "success" : metrics.cache_hit_rate > 0.6 ? "warning" : "error",
  };
});

// 格式化百分比
function formatPercentage(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

// 格式化数字
function formatNumber(value: number): string {
  if (value >= 1000000) {
    return `${(value / 1000000).toFixed(1)}M`;
  } else if (value >= 1000) {
    return `${(value / 1000).toFixed(1)}K`;
  }
  return value.toString();
}

// 触发池重填
function handlePoolRefill() {
  emit("operation", "pool_refill");
}
</script>

<template>
  <n-card title="池统计" :bordered="false">
    <template #header-extra>
      <n-space>
        <n-button size="small" @click="handlePoolRefill" :disabled="!group">
          <template #icon>
            <n-icon :component="SettingsOutline" />
          </template>
          重填池
        </n-button>
      </n-space>
    </template>

    <div v-if="!poolStats" class="loading-state">
      <p>暂无统计数据</p>
    </div>

    <div v-else class="pool-stats">
      <!-- 基础统计 -->
      <div class="stats-section">
        <h4 class="section-title">基础统计</h4>
        <n-grid :cols="4" :x-gap="16" :y-gap="16">
          <n-grid-item>
            <div class="stat-card">
              <div class="stat-header">
                <n-icon :component="BarChartOutline" size="18" />
                <span class="stat-title">总密钥数</span>
              </div>
              <div class="stat-value">
                {{ poolStats.total_keys }}
              </div>
            </div>
          </n-grid-item>
          
          <n-grid-item>
            <div class="stat-card">
              <div class="stat-header">
                <n-icon :component="TrendingUpOutline" size="18" />
                <span class="stat-title">活跃密钥</span>
              </div>
              <div class="stat-value">
                {{ poolStats.active_keys }}
              </div>
            </div>
          </n-grid-item>
          
          <n-grid-item>
            <div class="stat-card">
              <div class="stat-header">
                <n-icon :component="AlertCircleOutline" size="18" />
                <span class="stat-title">429密钥</span>
              </div>
              <div class="stat-value">
                {{ poolStats.rate_limited_keys }}
              </div>
            </div>
          </n-grid-item>
          
          <n-grid-item>
            <div class="stat-card">
              <div class="stat-header">
                <n-icon :component="SpeedometerOutline" size="18" />
                <span class="stat-title">可用率</span>
              </div>
              <div class="stat-value">
                {{ formatPercentage(poolStats.active_keys / Math.max(poolStats.total_keys, 1)) }}
              </div>
            </div>
          </n-grid-item>
        </n-grid>
      </div>

      <!-- 性能指标 -->
      <div class="stats-section" v-if="poolStats.performance_metrics">
        <h4 class="section-title">性能指标</h4>
        <n-grid :cols="2" :x-gap="24" :y-gap="16">
          <n-grid-item>
            <div class="metric-card">
              <div class="metric-header">
                <n-icon :component="AlertCircleOutline" />
                <span class="metric-title">错误率</span>
              </div>
              <div class="metric-value">
                {{ formatPercentage(poolStats.performance_metrics.error_rate) }}
              </div>
              <n-progress
                type="line"
                :percentage="poolStats.performance_metrics.error_rate * 100"
                :color="performanceColors.errorRate === 'success' ? '#10b981' : 
                       performanceColors.errorRate === 'warning' ? '#f59e0b' : '#ef4444'"
                :show-indicator="false"
                :height="4"
                style="margin-top: 8px"
              />
            </div>
          </n-grid-item>
          
          <n-grid-item>
            <div class="metric-card">
              <div class="metric-header">
                <n-icon :component="SpeedometerOutline" />
                <span class="metric-title">缓存命中率</span>
              </div>
              <div class="metric-value">
                {{ formatPercentage(poolStats.performance_metrics.cache_hit_rate) }}
              </div>
              <n-progress
                type="line"
                :percentage="poolStats.performance_metrics.cache_hit_rate * 100"
                :color="performanceColors.cacheHitRate === 'success' ? '#10b981' : 
                       performanceColors.cacheHitRate === 'warning' ? '#f59e0b' : '#ef4444'"
                :show-indicator="false"
                :height="4"
                style="margin-top: 8px"
              />
            </div>
          </n-grid-item>
        </n-grid>
      </div>

      <!-- 吞吐量统计 -->
      <div class="stats-section" v-if="poolStats.performance_metrics">
        <h4 class="section-title">吞吐量统计</h4>
        <n-grid :cols="3" :x-gap="16">
          <n-grid-item>
            <div class="throughput-card">
              <div class="throughput-label">当前吞吐量</div>
              <div class="throughput-value">
                {{ formatNumber(poolStats.performance_metrics.throughput) }}
                <span class="throughput-unit">req/s</span>
              </div>
            </div>
          </n-grid-item>
          
          <n-grid-item>
            <div class="throughput-card">
              <div class="throughput-label">平均延迟</div>
              <div class="throughput-value">
                {{ poolStats.performance_metrics.avg_latency.toFixed(1) }}
                <span class="throughput-unit">ms</span>
              </div>
            </div>
          </n-grid-item>
          
          <n-grid-item>
            <div class="throughput-card">
              <div class="throughput-label">总请求数</div>
              <div class="throughput-value">
                {{ formatNumber(poolStats.performance_metrics.total_requests) }}
              </div>
            </div>
          </n-grid-item>
        </n-grid>
      </div>
    </div>
  </n-card>
</template>

<style scoped>
.loading-state {
  text-align: center;
  padding: 40px;
  color: var(--text-color-3);
}

.pool-stats {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.stats-section {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.section-title {
  margin: 0;
  font-size: 16px;
  font-weight: 500;
  color: var(--text-color-1);
}

.stat-card {
  padding: 16px;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  background: var(--card-color);
  transition: all 0.3s ease;
}

.stat-card:hover {
  border-color: var(--primary-color);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

.stat-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}

.stat-title {
  font-size: 14px;
  color: var(--text-color-2);
}

.stat-value {
  font-size: 24px;
  font-weight: 600;
  color: var(--text-color-1);
}

.metric-card {
  padding: 20px;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  background: var(--card-color);
}

.metric-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
}

.metric-title {
  font-size: 14px;
  color: var(--text-color-2);
}

.metric-value {
  font-size: 20px;
  font-weight: 600;
  color: var(--text-color-1);
}

.throughput-card {
  padding: 16px;
  text-align: center;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  background: var(--card-color);
}

.throughput-label {
  font-size: 14px;
  color: var(--text-color-2);
  margin-bottom: 8px;
}

.throughput-value {
  font-size: 18px;
  font-weight: 600;
  color: var(--text-color-1);
}

.throughput-unit {
  font-size: 12px;
  color: var(--text-color-3);
  margin-left: 4px;
}
</style>
