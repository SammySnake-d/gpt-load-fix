<script setup lang="ts">
import type { Group, RecoveryMetrics } from "@/types/models";
import {
    CheckmarkCircleOutline,
    CloseCircleOutline,
    RefreshOutline
} from "@vicons/ionicons5";
import {
    NButton,
    NCard,
    NEmpty,
    NGrid,
    NGridItem,
    NIcon,
    NStatistic,
    NTable,
    NTag
} from "naive-ui";

interface Props {
  group: Group | null;
  recoveryMetrics: RecoveryMetrics | null;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const props = defineProps<Props>();

// 格式化时间
function formatTime(seconds: number): string {
  if (seconds < 60) {
    return `${seconds}秒`;
  } else if (seconds < 3600) {
    return `${Math.floor(seconds / 60)}分${seconds % 60}秒`;
  } else {
    return `${Math.floor(seconds / 3600)}小时${Math.floor((seconds % 3600) / 60)}分`;
  }
}

// 格式化百分比
function formatPercentage(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}
</script>

<template>
  <n-card title="恢复监控" :bordered="false">
    <template #header-extra>
      <n-button size="small" :disabled="!group">
        <template #icon>
          <n-icon :component="RefreshOutline" />
        </template>
        刷新数据
      </n-button>
    </template>

    <div v-if="!recoveryMetrics" class="loading-state">
      <n-empty description="暂无恢复数据" />
    </div>

    <div v-else class="recovery-monitor">
      <!-- 恢复统计 -->
      <div class="stats-section">
        <h4 class="section-title">恢复统计</h4>
        <n-grid :cols="4" :x-gap="16" :y-gap="16">
          <n-grid-item>
            <n-statistic
              label="总恢复次数"
              :value="recoveryMetrics.total_recoveries"
            />
          </n-grid-item>
          <n-grid-item>
            <n-statistic
              label="成功恢复"
              :value="recoveryMetrics.successful_recoveries"
            />
          </n-grid-item>
          <n-grid-item>
            <n-statistic
              label="成功率"
              :value="formatPercentage(recoveryMetrics.success_rate)"
            />
          </n-grid-item>
          <n-grid-item>
            <n-statistic
              label="平均恢复时间"
              :value="formatTime(recoveryMetrics.avg_recovery_time)"
            />
          </n-grid-item>
        </n-grid>
      </div>

      <!-- 恢复趋势 -->
      <div class="trends-section" v-if="recoveryMetrics.recent_recoveries">
        <h4 class="section-title">最近恢复记录</h4>
        <n-table :bordered="false" :single-line="false">
          <thead>
            <tr>
              <th>时间</th>
              <th>密钥数量</th>
              <th>恢复时间</th>
              <th>状态</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(recovery, index) in recoveryMetrics.recent_recoveries" :key="index">
              <td>{{ new Date(recovery.timestamp).toLocaleString() }}</td>
              <td>{{ recovery.key_count }}</td>
              <td>{{ formatTime(recovery.recovery_time) }}</td>
              <td>
                <n-tag :type="recovery.success ? 'success' : 'error'">
                  <template #icon>
                    <n-icon :component="recovery.success ? CheckmarkCircleOutline : CloseCircleOutline" />
                  </template>
                  {{ recovery.success ? '成功' : '失败' }}
                </n-tag>
              </td>
            </tr>
          </tbody>
        </n-table>
      </div>
    </div>
  </n-card>
</template>

<style scoped>
.loading-state {
  text-align: center;
  padding: 40px;
}

.recovery-monitor {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.section-title {
  margin: 0 0 16px 0;
  font-size: 16px;
  font-weight: 600;
  color: var(--text-color-1);
}

.stats-section {
  padding: 16px;
  background: var(--card-color);
  border-radius: 8px;
}

.trends-section {
  padding: 16px;
  background: var(--card-color);
  border-radius: 8px;
}
</style>
