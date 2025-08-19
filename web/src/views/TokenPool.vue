<script setup lang="ts">
import { keysApi } from "@/api/keys";
import { poolApi } from "@/api/pool";
import PoolOverview from "@/components/pool/PoolOverview.vue";
import PoolStats from "@/components/pool/PoolStats.vue";
import RecoveryControl from "@/components/pool/RecoveryControl.vue";
import RecoveryMonitor from "@/components/pool/RecoveryMonitor.vue";
import type { Group, PoolStatsResponse, RecoveryMetrics } from "@/types/models";
import { RefreshOutline } from "@vicons/ionicons5";
import {
    NButton,
    NCard,
    NIcon,
    NSelect,
    NSpace,
    NSwitch,
    useMessage,
} from "naive-ui";
import { computed, onMounted, onUnmounted, ref } from "vue";

const message = useMessage();
const loading = ref(false);
const groups = ref<Group[]>([]);
const selectedGroup = ref<Group | null>(null);
const selectedGroupId = ref<number | null>(null);
const poolStats = ref<PoolStatsResponse | null>(null);
const recoveryMetrics = ref<RecoveryMetrics | null>(null);
const autoRefresh = ref(true);
const refreshInterval = ref<number | null>(null);

// 分组选项
const groupOptions = computed(() => [
  { label: "选择分组", value: null, disabled: true, type: 'group' as const },
  ...groups.value.map(group => ({
    label: `${group.name} (${group.id})`,
    value: group.id,
  })),
]);

onMounted(async () => {
  await loadGroups();
});

onUnmounted(() => {
  if (refreshInterval.value) {
    clearInterval(refreshInterval.value);
  }
});

// 加载分组列表
async function loadGroups() {
  try {
    loading.value = true;
    groups.value = await keysApi.getGroups();
  } catch (error) {
    message.error("加载分组失败");
  } finally {
    loading.value = false;
  }
}

// 分组变化处理
function onGroupChange(groupId: number | null) {
  if (groupId) {
    selectedGroup.value = groups.value.find(g => g.id === groupId) || null;
    loadPoolData();
  } else {
    selectedGroup.value = null;
  }
}

// 加载池数据
async function loadPoolData() {
  if (!selectedGroup.value) return;

  try {
    loading.value = true;

    // 并行加载池统计和恢复指标
    const [statsRes, metricsRes] = await Promise.all([
      poolApi.getPoolStats(selectedGroup.value?.id || 0),
      poolApi.getRecoveryMetrics(selectedGroup.value?.id || 0),
    ]);

    poolStats.value = statsRes;
    recoveryMetrics.value = metricsRes;
  } catch (error) {
    message.error("加载池数据失败");
  } finally {
    loading.value = false;
  }
}

// 手动刷新
async function handleRefresh() {
  await loadPoolData();
  message.success("数据已刷新");
}

// 自动刷新切换
function toggleAutoRefresh(enabled: boolean) {
  autoRefresh.value = enabled;

  if (enabled) {
    refreshInterval.value = window.setInterval(() => {
      if (selectedGroup.value) {
        loadPoolData();
      }
    }, 30000); // 30秒刷新一次
  } else {
    if (refreshInterval.value) {
      clearInterval(refreshInterval.value);
      refreshInterval.value = null;
    }
  }
}

// 处理池操作
async function handlePoolOperation(operation: string, data?: any) {
  if (!selectedGroup.value) return;

  try {
    switch (operation) {
      case 'manual_recovery':
        await poolApi.triggerManualRecovery(selectedGroup.value?.id || 0, data.keyIds);
        message.success(`已触发 ${data.keyIds.length} 个密钥的手动恢复`);
        break;
      case 'batch_recovery':
        await poolApi.triggerBatchRecovery(selectedGroup.value?.id || 0, data);
        message.success("已触发批量恢复");
        break;
      case 'pool_refill':
        await poolApi.refillPools(selectedGroup.value?.id || 0);
        message.success("池重填完成");
        break;
    }

    // 刷新数据
    await loadPoolData();
  } catch (error) {
    message.error(`操作失败: ${error instanceof Error ? error.message : String(error)}`);
  }
}
</script>

<template>
  <div class="token-pool-container">
    <!-- 页面头部 -->
    <div class="page-header">
      <div class="header-left">
        <h1 class="page-title">Token池管理</h1>
        <p class="page-subtitle">分层密钥池状态监控与429智能恢复</p>
      </div>

      <div class="header-actions">
        <n-space :size="12">
          <!-- 分组选择 -->
          <n-select
            v-model:value="selectedGroupId"
            :options="groupOptions"
            placeholder="选择分组"
            style="width: 200px"
            :loading="loading"
            @update:value="onGroupChange"
          />

          <!-- 自动刷新开关 -->
          <n-space align="center" :size="8">
            <span class="refresh-label">自动刷新</span>
            <n-switch
              v-model:value="autoRefresh"
              @update:value="toggleAutoRefresh"
            />
          </n-space>

          <!-- 手动刷新按钮 -->
          <n-button
            type="primary"
            :loading="loading"
            @click="handleRefresh"
            :disabled="!selectedGroup"
          >
            <template #icon>
              <n-icon :component="RefreshOutline" />
            </template>
            刷新
          </n-button>
        </n-space>
      </div>
    </div>

    <!-- 主要内容 -->
    <div class="pool-content" v-if="selectedGroup">
      <!-- 池概览 -->
      <PoolOverview
        :group="selectedGroup"
        :pool-stats="poolStats"
        class="pool-section"
      />

      <!-- 池统计 -->
      <PoolStats
        :group="selectedGroup"
        :pool-stats="poolStats"
        @operation="handlePoolOperation"
        class="pool-section"
      />

      <!-- 恢复控制 -->
      <RecoveryControl
        :group="selectedGroup"
        :pool-stats="poolStats"
        @operation="handlePoolOperation"
        class="pool-section"
      />

      <!-- 恢复监控 -->
      <RecoveryMonitor
        :group="selectedGroup"
        :recovery-metrics="recoveryMetrics"
        class="pool-section"
      />


    </div>

    <!-- 空状态 -->
    <div class="empty-state" v-else>
      <n-card>
        <div class="empty-content">
          <h3>请选择一个分组</h3>
          <p>选择分组后即可查看Token池状态和恢复指标</p>
        </div>
      </n-card>
    </div>
  </div>
</template>

<style scoped>
.token-pool-container {
  padding: 24px;
  max-width: 1400px;
  margin: 0 auto;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 24px;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--border-color);
}

.header-left {
  flex: 1;
}

.page-title {
  margin: 0 0 8px 0;
  font-size: 28px;
  font-weight: 600;
  color: var(--text-color-1);
}

.page-subtitle {
  margin: 0;
  font-size: 14px;
  color: var(--text-color-3);
}

.header-actions {
  flex-shrink: 0;
}

.refresh-label {
  font-size: 14px;
  color: var(--text-color-2);
}

.pool-content {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.pool-section {
  width: 100%;
}

.empty-state {
  margin-top: 60px;
}

.empty-content {
  text-align: center;
  padding: 40px 20px;
}

.empty-content h3 {
  margin: 0 0 12px 0;
  font-size: 18px;
  color: var(--text-color-2);
}

.empty-content p {
  margin: 0;
  font-size: 14px;
  color: var(--text-color-3);
}
</style>
