<script setup lang="ts">
import type { Group, PoolStatsResponse } from "@/types/models";
import {
  CheckmarkCircleOutline,
  FlashOutline,
  RefreshOutline,
  TimeOutline,
} from "@vicons/ionicons5";
import { NCard, NGrid, NGridItem, NIcon, NProgress, NStatistic, NTag } from "naive-ui";
import { computed } from "vue";

interface Props {
  group: Group | null;
  poolStats: PoolStatsResponse | null;
}

const props = defineProps<Props>();

// 计算池状态颜色
const poolStatusColors = computed(() => {
  if (!props.poolStats) {
    return {};
  }

  const stats = props.poolStats;
  return {
    priority: (stats.priority_pool?.active_count || 0) > 0 ? "success" : "warning",
    ready: (stats.ready_pool?.active_count || 0) > 0 ? "success" : "warning",
    active: (stats.active_pool?.active_count || 0) > 0 ? "success" : "error",
    cooling: (stats.cooling_pool?.active_count || 0) > 0 ? "info" : "default",
  };
});

// 计算总体健康状态
const overallHealth = computed(() => {
  if (!props.poolStats) {
    return { status: "unknown", color: "default" };
  }

  const stats = props.poolStats;
  const totalActive =
    (stats.priority_pool?.active_count || 0) +
    (stats.ready_pool?.active_count || 0) +
    (stats.active_pool?.active_count || 0);

  if (totalActive === 0) {
    return { status: "critical", color: "error" };
  } else if (totalActive < 5) {
    return { status: "warning", color: "warning" };
  } else {
    return { status: "healthy", color: "success" };
  }
});
</script>

<template>
  <n-card title="池概览" :bordered="false">
    <template #header-extra>
      <n-tag
        :type="overallHealth.color as 'info' | 'success' | 'warning' | 'error' | 'default'"
        size="small"
      >
        {{
          overallHealth.status === "healthy"
            ? "健康"
            : overallHealth.status === "warning"
              ? "警告"
              : overallHealth.status === "critical"
                ? "严重"
                : "未知"
        }}
      </n-tag>
    </template>

    <div v-if="!poolStats" class="loading-state">
      <p>暂无数据</p>
    </div>

    <div v-else class="pool-overview">
      <!-- 池状态网格 -->
      <n-grid :cols="4" :x-gap="16" :y-gap="16" class="pool-grid">
        <!-- 优先池 -->
        <n-grid-item>
          <n-card size="small" :bordered="true" class="pool-card priority-pool">
            <div class="pool-header">
              <n-icon :component="FlashOutline" size="20" class="pool-icon priority" />
              <span class="pool-name">优先池</span>
            </div>
            <div class="pool-stats">
              <n-statistic
                label="活跃密钥"
                :value="poolStats.priority_pool?.active_count || 0"
                class="pool-stat"
              />
              <n-progress
                type="line"
                :percentage="
                  ((poolStats.priority_pool?.active_count || 0) /
                    Math.max(poolStats.priority_pool?.total_count || 1, 1)) *
                  100
                "
                :color="poolStatusColors.priority === 'success' ? '#10b981' : '#f59e0b'"
                :show-indicator="false"
                :height="4"
                style="margin-top: 8px"
              />
            </div>
          </n-card>
        </n-grid-item>

        <!-- 就绪池 -->
        <n-grid-item>
          <n-card size="small" :bordered="true" class="pool-card ready-pool">
            <div class="pool-header">
              <n-icon :component="CheckmarkCircleOutline" size="20" class="pool-icon ready" />
              <span class="pool-name">就绪池</span>
            </div>
            <div class="pool-stats">
              <n-statistic
                label="就绪密钥"
                :value="poolStats.ready_pool?.active_count || 0"
                class="pool-stat"
              />
              <n-progress
                type="line"
                :percentage="
                  ((poolStats.ready_pool?.active_count || 0) /
                    Math.max(poolStats.ready_pool?.total_count || 1, 1)) *
                  100
                "
                :color="poolStatusColors.ready === 'success' ? '#10b981' : '#f59e0b'"
                :show-indicator="false"
                :height="4"
                style="margin-top: 8px"
              />
            </div>
          </n-card>
        </n-grid-item>

        <!-- 活跃池 -->
        <n-grid-item>
          <n-card size="small" :bordered="true" class="pool-card active-pool">
            <div class="pool-header">
              <n-icon :component="RefreshOutline" size="20" class="pool-icon active" />
              <span class="pool-name">活跃池</span>
            </div>
            <div class="pool-stats">
              <n-statistic
                label="使用中密钥"
                :value="poolStats.active_pool?.active_count || 0"
                class="pool-stat"
              />
              <n-progress
                type="line"
                :percentage="
                  ((poolStats.active_pool?.active_count || 0) /
                    Math.max(poolStats.active_pool?.total_count || 1, 1)) *
                  100
                "
                :color="poolStatusColors.active === 'success' ? '#10b981' : '#ef4444'"
                :show-indicator="false"
                :height="4"
                style="margin-top: 8px"
              />
            </div>
          </n-card>
        </n-grid-item>

        <!-- 冷却池 -->
        <n-grid-item>
          <n-card size="small" :bordered="true" class="pool-card cooling-pool">
            <div class="pool-header">
              <n-icon :component="TimeOutline" size="20" class="pool-icon cooling" />
              <span class="pool-name">冷却池</span>
            </div>
            <div class="pool-stats">
              <n-statistic
                label="冷却密钥"
                :value="poolStats.cooling_pool?.active_count || 0"
                class="pool-stat"
              />
              <n-progress
                type="line"
                :percentage="
                  ((poolStats.cooling_pool?.active_count || 0) /
                    Math.max(poolStats.cooling_pool?.total_count || 1, 1)) *
                  100
                "
                color="#6366f1"
                :show-indicator="false"
                :height="4"
                style="margin-top: 8px"
              />
            </div>
          </n-card>
        </n-grid-item>
      </n-grid>

      <!-- 性能指标 -->
      <div class="performance-section" v-if="poolStats.performance_metrics">
        <h4 class="section-title">性能指标</h4>
        <n-grid :cols="3" :x-gap="16" class="metrics-grid">
          <n-grid-item>
            <n-statistic
              label="吞吐量"
              :value="poolStats.performance_metrics.throughput"
              suffix="req/s"
            />
          </n-grid-item>
          <n-grid-item>
            <n-statistic
              label="平均延迟"
              :value="poolStats.performance_metrics.avg_latency"
              suffix="ms"
            />
          </n-grid-item>
          <n-grid-item>
            <n-statistic
              label="错误率"
              :value="(poolStats.performance_metrics.error_rate * 100).toFixed(2)"
              suffix="%"
            />
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

.pool-overview {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.pool-grid {
  width: 100%;
}

.pool-card {
  height: 100%;
  transition: all 0.3s ease;
}

.pool-card:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
}

.pool-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
}

.pool-name {
  font-weight: 500;
  font-size: 14px;
}

.pool-icon {
  border-radius: 50%;
  padding: 4px;
}

.pool-icon.priority {
  background-color: #fef3c7;
  color: #f59e0b;
}

.pool-icon.ready {
  background-color: #d1fae5;
  color: #10b981;
}

.pool-icon.active {
  background-color: #dbeafe;
  color: #3b82f6;
}

.pool-icon.cooling {
  background-color: #e0e7ff;
  color: #6366f1;
}

.pool-stats {
  display: flex;
  flex-direction: column;
}

.pool-stat {
  margin-bottom: 8px;
}

.performance-section {
  margin-top: 16px;
}

.section-title {
  margin: 0 0 16px 0;
  font-size: 16px;
  font-weight: 500;
  color: var(--text-color-1);
}

.metrics-grid {
  width: 100%;
}
</style>
