<script setup lang="ts">
import type { BatchRecoveryRequest, Group, PoolStatsResponse } from "@/types/models";
import {
    FlashOutline,
    PlayOutline,
    RefreshOutline,
    SettingsOutline,
    StopOutline
} from "@vicons/ionicons5";
import {
    NAlert,
    NButton,
    NCard,
    NForm,
    NFormItem,
    NIcon,
    NInputNumber,
    NSelect,
    NSpace,
    NTag,
    NTooltip,
    useDialog,
    useMessage
} from "naive-ui";
import { computed, ref } from "vue";

interface Props {
  group: Group | null;
  poolStats: PoolStatsResponse | null;
}

interface Emits {
  (e: "operation", operation: string, data?: any): void;
}

const props = defineProps<Props>();
const emit = defineEmits<Emits>();
const message = useMessage();
const dialog = useDialog();

// 批量恢复表单
const batchRecoveryForm = ref<BatchRecoveryRequest>({
  max_keys: 10,
  priority_level: "normal",
  force_recovery: false,
});

// 优先级选项
const priorityOptions = [
  { label: "低优先级", value: "low" },
  { label: "普通优先级", value: "normal" },
  { label: "高优先级", value: "high" },
];

// 计算恢复建议
const recoveryRecommendation = computed(() => {
  if (!props.poolStats) return null;
  
  const rateLimitedCount = props.poolStats.rate_limited_keys;
  const activeCount = props.poolStats.active_keys;
  const totalCount = props.poolStats.total_keys;
  
  if (rateLimitedCount === 0) {
    return {
      type: "success",
      message: "当前没有需要恢复的密钥",
      action: null,
    };
  }
  
  if (activeCount < totalCount * 0.3) {
    return {
      type: "error",
      message: `检测到 ${rateLimitedCount} 个429密钥，可用密钥不足30%，建议立即批量恢复`,
      action: "batch_recovery",
    };
  }
  
  if (rateLimitedCount > 5) {
    return {
      type: "warning",
      message: `检测到 ${rateLimitedCount} 个429密钥，建议进行批量恢复`,
      action: "batch_recovery",
    };
  }
  
  return {
    type: "info",
    message: `检测到 ${rateLimitedCount} 个429密钥，可以手动恢复或等待自动恢复`,
    action: "manual_recovery",
  };
});

// 触发批量恢复
function handleBatchRecovery() {
  if (!props.group) return;
  
  dialog.warning({
    title: "确认批量恢复",
    content: `将尝试恢复最多 ${batchRecoveryForm.value.max_keys} 个429密钥，是否继续？`,
    positiveText: "确认",
    negativeText: "取消",
    onPositiveClick: () => {
      emit("operation", "batch_recovery", batchRecoveryForm.value);
    },
  });
}

// 触发手动恢复所有429密钥
function handleRecoverAll() {
  if (!props.group || !props.poolStats) return;
  
  const rateLimitedCount = props.poolStats.rate_limited_keys;
  if (rateLimitedCount === 0) {
    message.warning("当前没有需要恢复的密钥");
    return;
  }
  
  dialog.warning({
    title: "确认恢复所有429密钥",
    content: `将尝试恢复所有 ${rateLimitedCount} 个429密钥，这可能会导致再次触发429错误，是否继续？`,
    positiveText: "确认",
    negativeText: "取消",
    onPositiveClick: () => {
      // 这里需要获取所有429密钥的ID，简化处理
      emit("operation", "manual_recovery", { keyIds: [] });
    },
  });
}

// 快速恢复建议操作
function handleQuickAction() {
  if (!recoveryRecommendation.value?.action) return;
  
  if (recoveryRecommendation.value.action === "batch_recovery") {
    handleBatchRecovery();
  } else if (recoveryRecommendation.value.action === "manual_recovery") {
    handleRecoverAll();
  }
}
</script>

<template>
  <n-card title="恢复控制" :bordered="false">
    <div class="recovery-control">
      <!-- 恢复建议 -->
      <div class="recommendation-section" v-if="recoveryRecommendation">
        <n-alert
          :type="recoveryRecommendation.type"
          :title="recoveryRecommendation.type === 'success' ? '状态良好' : '恢复建议'"
          closable
        >
          {{ recoveryRecommendation.message }}
          <template #action v-if="recoveryRecommendation.action">
            <n-button
              size="small"
              :type="recoveryRecommendation.type === 'error' ? 'error' : 'primary'"
              @click="handleQuickAction"
            >
              快速执行
            </n-button>
          </template>
        </n-alert>
      </div>

      <!-- 批量恢复控制 -->
      <div class="control-section">
        <h4 class="section-title">批量恢复</h4>
        <n-form :model="batchRecoveryForm" label-placement="left" label-width="120px">
          <n-space vertical :size="16">
            <n-form-item label="最大恢复数量">
              <n-input-number
                v-model:value="batchRecoveryForm.max_keys"
                :min="1"
                :max="100"
                style="width: 200px"
              />
            </n-form-item>
            
            <n-form-item label="优先级">
              <n-select
                v-model:value="batchRecoveryForm.priority_level"
                :options="priorityOptions"
                style="width: 200px"
              />
            </n-form-item>
            
            <n-form-item>
              <n-space>
                <n-button
                  type="primary"
                  @click="handleBatchRecovery"
                  :disabled="!group || !poolStats?.rate_limited_keys"
                >
                  <template #icon>
                    <n-icon :component="PlayOutline" />
                  </template>
                  开始批量恢复
                </n-button>
                
                <n-button
                  @click="handleRecoverAll"
                  :disabled="!group || !poolStats?.rate_limited_keys"
                >
                  <template #icon>
                    <n-icon :component="RefreshOutline" />
                  </template>
                  恢复所有429密钥
                </n-button>
              </n-space>
            </n-form-item>
          </n-space>
        </n-form>
      </div>

      <!-- 状态信息 -->
      <div class="status-section" v-if="poolStats">
        <h4 class="section-title">当前状态</h4>
        <n-space>
          <n-tag type="info">
            <template #icon>
              <n-icon :component="SettingsOutline" />
            </template>
            总密钥: {{ poolStats.total_keys }}
          </n-tag>
          
          <n-tag type="success">
            <template #icon>
              <n-icon :component="PlayOutline" />
            </template>
            活跃: {{ poolStats.active_keys }}
          </n-tag>
          
          <n-tag type="error" v-if="poolStats.rate_limited_keys > 0">
            <template #icon>
              <n-icon :component="StopOutline" />
            </template>
            429限制: {{ poolStats.rate_limited_keys }}
          </n-tag>
          
          <n-tag type="warning" v-if="poolStats.cooling_pool?.active_count > 0">
            <template #icon>
              <n-icon :component="FlashOutline" />
            </template>
            冷却中: {{ poolStats.cooling_pool.active_count }}
          </n-tag>
        </n-space>
      </div>
    </div>
  </n-card>
</template>

<style scoped>
.recovery-control {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.recommendation-section {
  width: 100%;
}

.control-section {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.status-section {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.section-title {
  margin: 0;
  font-size: 16px;
  font-weight: 500;
  color: var(--text-color-1);
}
</style>
