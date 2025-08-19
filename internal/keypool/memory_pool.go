package keypool

import (
	"context"
	"fmt"
	"gpt-load/internal/config"
	"gpt-load/internal/errors"
	"gpt-load/internal/models"
	"gpt-load/internal/store"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// MemoryLayeredPool 基于分片内存存储的分层密钥池
type MemoryLayeredPool struct {
	// 依赖
	shardedStore    *ShardedMemoryStore
	db              *gorm.DB
	settingsManager *config.SystemSettingsManager

	// 配置
	config       *PoolConfig
	memoryConfig *MemoryPoolConfig

	// 组件
	metrics      PoolMetrics
	validator    KeyValidator
	eventHandler EventHandler
	errorHandler ErrorHandler

	// 429恢复组件
	recoveryService    *RecoveryService
	recoveryMonitor    *RecoveryMonitor
	recoveryStrategy   RecoveryStrategy
	batchProcessor     *BatchRecoveryProcessor

	// 性能监控
	performanceMonitor *PerformanceMonitor

	// 运行时状态
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	started    bool
	mu         sync.RWMutex

	// 缓存和优化
	groupConfigs map[uint]*PoolConfig
	configMu     sync.RWMutex
	localCache   *localKeyCache
}

// localKeyCache 本地密钥缓存
type localKeyCache struct {
	mu       sync.RWMutex
	cache    map[uint]*cacheEntry
	maxSize  int
	ttl      time.Duration
}

// cacheEntry 缓存条目
type cacheEntry struct {
	key       *models.APIKey
	expiresAt time.Time
}

// LocalCacheConfig 本地缓存配置
type LocalCacheConfig struct {
	MaxSize         int           `json:"max_size"`
	TTL             time.Duration `json:"ttl"`
	CleanupInterval time.Duration `json:"cleanup_interval"`
	EnableMetrics   bool          `json:"enable_metrics"`
}

// NewMemoryLayeredPool 创建内存分层密钥池
func NewMemoryLayeredPool(factoryConfig *FactoryConfig) (*MemoryLayeredPool, error) {
	if factoryConfig.DB == nil {
		return nil, NewPoolError(ErrorTypeConfiguration, "MISSING_DB", "Database is required")
	}

	db, ok := factoryConfig.DB.(*gorm.DB)
	if !ok {
		return nil, NewPoolError(ErrorTypeConfiguration, "INVALID_DB", "DB must be *gorm.DB")
	}

	settingsManager, ok := factoryConfig.SettingsManager.(*config.SystemSettingsManager)
	if !ok {
		return nil, NewPoolError(ErrorTypeConfiguration, "INVALID_SETTINGS_MANAGER", "SettingsManager must be *config.SystemSettingsManager")
	}

	memoryConfig := factoryConfig.MemoryConfig
	if memoryConfig == nil {
		memoryConfig = DefaultMemoryPoolConfig()
	}

	// 创建分片存储配置
	shardedConfig := &ShardedStoreConfig{
		ShardCount:     memoryConfig.ShardCount,
		LockTimeout:    memoryConfig.LockTimeout,
		GCInterval:     memoryConfig.GCInterval,
		MaxMemoryUsage: memoryConfig.MaxMemoryUsage,
		EnableMetrics:  true,
		CacheSize:      1000,
	}

	// 创建分片存储
	shardedStore, err := NewShardedMemoryStore(shardedConfig)
	if err != nil {
		return nil, NewPoolErrorWithCause(ErrorTypeConfiguration, "SHARDED_STORE_FAILED", "Failed to create sharded store", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &MemoryLayeredPool{
		shardedStore:    shardedStore,
		db:              db,
		settingsManager: settingsManager,
		config:          factoryConfig.DefaultPoolConfig,
		memoryConfig:    memoryConfig,
		metrics:         factoryConfig.Metrics,
		validator:       factoryConfig.Validator,
		eventHandler:    factoryConfig.EventHandler,
		ctx:             ctx,
		cancel:          cancel,
		groupConfigs:    make(map[uint]*PoolConfig),
	}

	// 创建本地缓存
	if memoryConfig.EnableSharding {
		pool.localCache = &localKeyCache{
			cache:   make(map[uint]*cacheEntry),
			maxSize: memoryConfig.ShardCount * 100, // 每个分片100个缓存项
			ttl:     5 * time.Minute,
		}
	}

	// 创建默认错误处理器
	if pool.errorHandler == nil {
		pool.errorHandler = &DefaultErrorHandler{}
	}

	// 初始化429恢复组件
	if err := pool.initializeRecoveryComponents(); err != nil {
		return nil, NewPoolErrorWithCause(ErrorTypeConfiguration, "RECOVERY_INIT_FAILED", "Failed to initialize recovery components", err)
	}

	return pool, nil
}

// Start 启动内存分层池
func (p *MemoryLayeredPool) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return NewPoolError(ErrorTypeConfiguration, "ALREADY_STARTED", "Pool is already started")
	}

	// 启动后台任务
	p.wg.Add(1)
	go p.maintenanceLoop()

	if p.localCache != nil {
		p.wg.Add(1)
		go p.cacheCleanupLoop()
	}

	p.started = true
	logrus.Info("Memory layered pool started")

	return nil
}

// Stop 停止内存分层池
func (p *MemoryLayeredPool) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return nil
	}

	p.cancel()
	p.wg.Wait()

	// 关闭分片存储
	if err := p.shardedStore.Close(); err != nil {
		logrus.WithError(err).Warn("Failed to close sharded store")
	}

	p.started = false
	logrus.Info("Memory layered pool stopped")

	return nil
}

// Health 检查池健康状态
func (p *MemoryLayeredPool) Health() error {
	// 检查分片存储
	if err := p.shardedStore.Set("health_check", []byte("ok"), 10*time.Second); err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "MEMORY_UNAVAILABLE", "Memory store health check failed", err)
	}

	// 检查数据库连接
	sqlDB, err := p.db.DB()
	if err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "DB_UNAVAILABLE", "Database connection failed", err)
	}

	if err := sqlDB.Ping(); err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "DB_PING_FAILED", "Database ping failed", err)
	}

	return nil
}

// getGroupConfig 获取分组配置
func (p *MemoryLayeredPool) getGroupConfig(groupID uint) *PoolConfig {
	p.configMu.RLock()
	config, exists := p.groupConfigs[groupID]
	p.configMu.RUnlock()

	if exists {
		return config
	}

	// 使用默认配置
	defaultConfig := DefaultPoolConfig(groupID)

	p.configMu.Lock()
	p.groupConfigs[groupID] = defaultConfig
	p.configMu.Unlock()

	return defaultConfig
}

// UpdateConfig 更新分组配置
func (p *MemoryLayeredPool) UpdateConfig(groupID uint, config *PoolConfig) error {
	if config == nil {
		return NewPoolError(ErrorTypeConfiguration, "NIL_CONFIG", "Config cannot be nil")
	}

	config.GroupID = groupID

	p.configMu.Lock()
	p.groupConfigs[groupID] = config
	p.configMu.Unlock()

	logrus.WithField("groupID", groupID).Info("Memory pool configuration updated")

	return nil
}

// GetConfig 获取分组配置
func (p *MemoryLayeredPool) GetConfig(groupID uint) (*PoolConfig, error) {
	config := p.getGroupConfig(groupID)
	return config, nil
}

// SelectKey 选择一个可用的密钥
func (p *MemoryLayeredPool) SelectKey(groupID uint) (*models.APIKey, error) {
	startTime := time.Now()
	var success bool
	defer func() {
		if p.performanceMonitor != nil {
			p.performanceMonitor.RecordRequest(success, time.Since(startTime))
		}
	}()

	// 尝试从本地缓存获取
	if p.localCache != nil {
		if cachedKey := p.getCachedKey(groupID); cachedKey != nil {
			// 记录缓存命中
			if p.performanceMonitor != nil {
				p.performanceMonitor.RecordCacheHit()
			}

			// 记录性能指标
			latency := time.Since(startTime)
			if p.metrics != nil {
				p.metrics.RecordKeySelection(groupID, latency, true)
			}

			success = true
			return cachedKey, nil
		} else {
			// 记录缓存未命中
			if p.performanceMonitor != nil {
				p.performanceMonitor.RecordCacheMiss()
			}
		}
	}

	// 从活跃池获取密钥
	activeKey := p.getRedisKey(groupID, PoolTypeActive)
	keyIDStr, err := p.shardedStore.Rotate(activeKey)

	if err != nil {
		if err == store.ErrNotFound {
			// 活跃池为空，尝试补充
			if err := p.RefillPools(groupID); err != nil {
				return nil, NewPoolErrorWithCause(ErrorTypeCapacity, "REFILL_FAILED", "Failed to refill pools", err)
			}

			// 再次尝试
			keyIDStr, err = p.shardedStore.Rotate(activeKey)
			if err != nil {
				if err == store.ErrNotFound {
					return nil, ErrPoolEmpty
				}
				return nil, NewPoolErrorWithCause(ErrorTypeStorage, "SELECT_FAILED", "Failed to select key", err)
			}
		} else {
			return nil, NewPoolErrorWithCause(ErrorTypeStorage, "SELECT_FAILED", "Failed to select key", err)
		}
	}

	keyID, err := strconv.ParseUint(keyIDStr, 10, 64)
	if err != nil {
		return nil, NewPoolErrorWithCause(ErrorTypeInternal, "INVALID_KEY_ID", "Invalid key ID format", err)
	}

	// 获取密钥详情
	details, err := p.getKeyDetails(uint(keyID))
	if err != nil {
		return nil, NewPoolErrorWithCause(ErrorTypeStorage, "KEY_DETAILS_FAILED", "Failed to get key details", err)
	}

	// 构造APIKey对象
	apiKey, err := p.buildAPIKeyFromDetails(uint(keyID), groupID, details)
	if err != nil {
		return nil, err
	}

	// 添加到本地缓存
	if p.localCache != nil {
		p.setCachedKey(uint(keyID), apiKey)
	}

	// 记录性能指标
	latency := time.Since(startTime)
	if p.metrics != nil {
		p.metrics.RecordKeySelection(groupID, latency, true)
	}

	// 发送事件
	if p.eventHandler != nil {
		event := &KeyPoolEvent{
			Type:      EventKeySelected,
			GroupID:   groupID,
			KeyID:     uint(keyID),
			PoolType:  PoolTypeActive,
			Message:   "Key selected successfully",
			Timestamp: time.Now(),
		}
		p.eventHandler.HandleEvent(event)
	}

	success = true
	return apiKey, nil
}

// ReturnKey 归还密钥
func (p *MemoryLayeredPool) ReturnKey(keyID uint, success bool) error {
	// 获取密钥详情以确定分组
	details, err := p.getKeyDetails(keyID)
	if err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "KEY_DETAILS_FAILED", "Failed to get key details", err)
	}

	groupIDStr, exists := details["group_id"]
	if !exists {
		return NewPoolError(ErrorTypeValidation, "MISSING_GROUP_ID", "Key details missing group_id")
	}

	groupID, err := strconv.ParseUint(groupIDStr, 10, 64)
	if err != nil {
		return NewPoolErrorWithCause(ErrorTypeInternal, "INVALID_GROUP_ID", "Invalid group ID format", err)
	}

	if success {
		// 成功使用，将密钥放回活跃池
		activeKey := p.getRedisKey(uint(groupID), PoolTypeActive)
		if err := p.shardedStore.LPush(activeKey, keyID); err != nil {
			return NewPoolErrorWithCause(ErrorTypeStorage, "RETURN_FAILED", "Failed to return key to active pool", err)
		}

		// 更新成功统计
		p.updateKeyStats(keyID, true)
	} else {
		// 使用失败，需要进一步处理
		return p.handleKeyFailure(keyID, uint(groupID))
	}

	// 发送事件
	if p.eventHandler != nil {
		event := &KeyPoolEvent{
			Type:      EventKeyReturned,
			GroupID:   uint(groupID),
			KeyID:     keyID,
			Message:   fmt.Sprintf("Key returned with success=%v", success),
			Timestamp: time.Now(),
		}
		p.eventHandler.HandleEvent(event)
	}

	return nil
}

// HandleRateLimit 处理429错误
func (p *MemoryLayeredPool) HandleRateLimit(keyID uint, rateLimitErr *errors.RateLimitError) error {
	// 记录429错误
	if p.performanceMonitor != nil {
		p.performanceMonitor.RecordRateLimit()
	}

	// 获取密钥详情
	details, err := p.getKeyDetails(keyID)
	if err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "KEY_DETAILS_FAILED", "Failed to get key details", err)
	}

	groupIDStr, exists := details["group_id"]
	if !exists {
		return NewPoolError(ErrorTypeValidation, "MISSING_GROUP_ID", "Key details missing group_id")
	}

	groupID, err := strconv.ParseUint(groupIDStr, 10, 64)
	if err != nil {
		return NewPoolErrorWithCause(ErrorTypeInternal, "INVALID_GROUP_ID", "Invalid group ID format", err)
	}

	// 从活跃池移除密钥
	activeKey := p.getRedisKey(uint(groupID), PoolTypeActive)
	if err := p.shardedStore.LRem(activeKey, 0, keyID); err != nil {
		logrus.WithFields(logrus.Fields{"keyID": keyID, "error": err}).Warn("Failed to remove rate-limited key from active pool")
	}

	// 添加到冷却池
	resetAt := time.Now().Add(rateLimitErr.RetryAfter)
	if rateLimitErr.ResetAt != nil {
		resetAt = *rateLimitErr.ResetAt
	}

	if err := p.addToCoolingPool(uint(groupID), keyID, resetAt); err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "COOLING_FAILED", "Failed to add key to cooling pool", err)
	}

	// 更新密钥详情
	now := time.Now()
	updates := map[string]interface{}{
		"status":               models.KeyStatusRateLimited,
		"rate_limit_count":     p.incrementRateLimitCount(details),
		"last_429_at":          now.Unix(),
		"rate_limit_reset_at":  resetAt.Unix(),
	}

	if err := p.setKeyDetails(keyID, updates); err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "UPDATE_DETAILS_FAILED", "Failed to update key details", err)
	}

	// 从本地缓存移除
	if p.localCache != nil {
		p.removeCachedKey(keyID)
	}

	// 记录指标
	if p.metrics != nil {
		p.metrics.RecordRateLimit(uint(groupID), keyID)
	}

	// 发送事件
	if p.eventHandler != nil {
		event := &KeyPoolEvent{
			Type:      EventRateLimitHit,
			GroupID:   uint(groupID),
			KeyID:     keyID,
			Message:   fmt.Sprintf("Key rate limited, reset at %v", resetAt),
			Timestamp: time.Now(),
			Metadata:  rateLimitErr,
		}
		p.eventHandler.HandleEvent(event)
	}

	logrus.WithFields(logrus.Fields{
		"keyID":      keyID,
		"groupID":    groupID,
		"retryAfter": rateLimitErr.RetryAfter,
		"resetAt":    resetAt,
	}).Info("Key moved to cooling pool due to rate limit")

	return nil
}

// AddKeys 添加密钥到池
func (p *MemoryLayeredPool) AddKeys(groupID uint, keyIDs []uint) error {
	if len(keyIDs) == 0 {
		return nil
	}

	// 默认添加到验证池
	return p.addKeysToPool(groupID, keyIDs, PoolTypeValidation)
}

// RemoveKeys 从池中移除密钥
func (p *MemoryLayeredPool) RemoveKeys(groupID uint, keyIDs []uint) error {
	if len(keyIDs) == 0 {
		return nil
	}

	// 从所有池中移除这些密钥
	poolTypes := []PoolType{PoolTypeValidation, PoolTypeReady, PoolTypeActive, PoolTypeCooling}

	for _, poolType := range poolTypes {
		if err := p.removeKeysFromPool(groupID, keyIDs, poolType); err != nil {
			logrus.WithFields(logrus.Fields{
				"groupID":  groupID,
				"poolType": poolType,
				"error":    err,
			}).Warn("Failed to remove keys from pool")
		}
	}

	// 删除密钥详情和缓存
	for _, keyID := range keyIDs {
		detailsKey := p.getKeyDetailsKey(keyID)
		if err := p.shardedStore.Delete(detailsKey); err != nil {
			logrus.WithFields(logrus.Fields{"keyID": keyID, "error": err}).Warn("Failed to delete key details")
		}

		if p.localCache != nil {
			p.removeCachedKey(keyID)
		}
	}

	return nil
}

// removeKeysFromPool 从指定池移除密钥的内部方法
func (p *MemoryLayeredPool) removeKeysFromPool(groupID uint, keyIDs []uint, poolType PoolType) error {
	if len(keyIDs) == 0 {
		return nil
	}

	switch poolType {
	case PoolTypeValidation:
		return p.removeFromValidationPool(groupID, keyIDs)
	case PoolTypeReady:
		return p.removeFromReadyPool(groupID, keyIDs)
	case PoolTypeActive:
		return p.removeFromActivePool(groupID, keyIDs)
	case PoolTypeCooling:
		return p.removeFromCoolingPool(groupID, keyIDs)
	default:
		return NewPoolError(ErrorTypeValidation, "UNKNOWN_POOL_TYPE", "Unknown pool type")
	}
}

// removeFromValidationPool 从验证池移除密钥
func (p *MemoryLayeredPool) removeFromValidationPool(groupID uint, keyIDs []uint) error {
	validationKey := p.getRedisKey(groupID, PoolTypeValidation)

	// 转换为interface{}切片
	members := make([]interface{}, len(keyIDs))
	for i, keyID := range keyIDs {
		members[i] = keyID
	}

	return p.shardedStore.SRem(validationKey, members...)
}

// removeFromReadyPool 从就绪池移除密钥
func (p *MemoryLayeredPool) removeFromReadyPool(groupID uint, keyIDs []uint) error {
	readyKey := p.getRedisKey(groupID, PoolTypeReady)

	for _, keyID := range keyIDs {
		if err := p.shardedStore.LRem(readyKey, 0, keyID); err != nil {
			return err
		}
	}

	return nil
}

// removeFromActivePool 从活跃池移除密钥
func (p *MemoryLayeredPool) removeFromActivePool(groupID uint, keyIDs []uint) error {
	activeKey := p.getRedisKey(groupID, PoolTypeActive)

	for _, keyID := range keyIDs {
		if err := p.shardedStore.LRem(activeKey, 0, keyID); err != nil {
			return err
		}
	}

	return nil
}

// MoveKey 在不同池之间移动密钥
func (p *MemoryLayeredPool) MoveKey(keyID uint, fromPool, toPool PoolType) error {
	// 获取密钥详情以确定分组
	details, err := p.getKeyDetails(keyID)
	if err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "KEY_DETAILS_FAILED", "Failed to get key details", err)
	}

	groupIDStr, exists := details["group_id"]
	if !exists {
		return NewPoolError(ErrorTypeValidation, "MISSING_GROUP_ID", "Key details missing group_id")
	}

	groupID, err := strconv.ParseUint(groupIDStr, 10, 64)
	if err != nil {
		return NewPoolErrorWithCause(ErrorTypeInternal, "INVALID_GROUP_ID", "Invalid group ID format", err)
	}

	// 验证池类型
	if fromPool == toPool {
		return NewPoolError(ErrorTypeValidation, "SAME_POOL", "Source and destination pools are the same")
	}

	// 执行移动操作
	return p.executeAtomicMove(keyID, uint(groupID), fromPool, toPool)
}

// executeAtomicMove 执行原子性的密钥移动
func (p *MemoryLayeredPool) executeAtomicMove(keyID, groupID uint, fromPool, toPool PoolType) error {
	// 首先从源池移除
	if err := p.removeKeysFromPool(groupID, []uint{keyID}, fromPool); err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "REMOVE_FROM_SOURCE_FAILED",
			fmt.Sprintf("Failed to remove key from %s pool", fromPool), err)
	}

	// 然后添加到目标池
	if err := p.addKeysToPool(groupID, []uint{keyID}, toPool); err != nil {
		// 移动失败，尝试回滚到源池
		if rollbackErr := p.addKeysToPool(groupID, []uint{keyID}, fromPool); rollbackErr != nil {
			logrus.WithFields(logrus.Fields{
				"keyID":       keyID,
				"fromPool":    fromPool,
				"toPool":      toPool,
				"rollbackErr": rollbackErr,
			}).Error("Failed to rollback key move operation")
		}

		return NewPoolErrorWithCause(ErrorTypeStorage, "ADD_TO_TARGET_FAILED",
			fmt.Sprintf("Failed to add key to %s pool", toPool), err)
	}

	// 发送事件
	if p.eventHandler != nil {
		event := &KeyPoolEvent{
			Type:      EventKeyMoved,
			GroupID:   groupID,
			KeyID:     keyID,
			PoolType:  toPool,
			Message:   fmt.Sprintf("Key moved from %s to %s", fromPool, toPool),
			Timestamp: time.Now(),
			Metadata: map[string]interface{}{
				"fromPool": fromPool,
				"toPool":   toPool,
			},
		}
		p.eventHandler.HandleEvent(event)
	}

	logrus.WithFields(logrus.Fields{
		"keyID":    keyID,
		"groupID":  groupID,
		"fromPool": fromPool,
		"toPool":   toPool,
	}).Info("Key moved between pools")

	return nil
}

// RefillPools 智能池补充机制
func (p *MemoryLayeredPool) RefillPools(groupID uint) error {
	config := p.getGroupConfig(groupID)

	// 检查活跃池是否需要补充
	activeCount, err := p.getPoolSize(groupID, PoolTypeActive)
	if err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "GET_ACTIVE_COUNT_FAILED", "Failed to get active pool size", err)
	}

	// 计算需要补充的数量
	needRefill := config.MinActiveKeys - int(activeCount)
	if needRefill <= 0 {
		return nil // 不需要补充
	}

	// 限制单次补充数量
	if needRefill > config.RefillBatchSize {
		needRefill = config.RefillBatchSize
	}

	// 尝试从就绪池补充到活跃池
	movedKeys, err := p.moveToActivePool(groupID, needRefill)
	if err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "MOVE_TO_ACTIVE_FAILED", "Failed to move keys to active pool", err)
	}

	actualMoved := len(movedKeys)
	if actualMoved > 0 {
		logrus.WithFields(logrus.Fields{
			"groupID":     groupID,
			"moved":       actualMoved,
			"needed":      needRefill,
			"activeCount": activeCount,
		}).Info("Refilled active pool from ready pool")

		// 记录指标
		if p.metrics != nil {
			p.metrics.RecordPoolRefill(groupID, actualMoved)
		}
	}

	return nil
}

// RecoverCooledKeys 恢复已过期的冷却密钥
func (p *MemoryLayeredPool) RecoverCooledKeys(groupID uint) (int, error) {
	// 获取已过期的冷却密钥
	expiredKeys, err := p.getExpiredFromCoolingPool(groupID)
	if err != nil {
		return 0, NewPoolErrorWithCause(ErrorTypeStorage, "GET_EXPIRED_KEYS_FAILED", "Failed to get expired keys from cooling pool", err)
	}

	if len(expiredKeys) == 0 {
		return 0, nil
	}

	recoveredCount := 0
	for _, keyID := range expiredKeys {
		if err := p.recoverSingleCooledKey(groupID, keyID); err != nil {
			logrus.WithFields(logrus.Fields{
				"groupID": groupID,
				"keyID":   keyID,
				"error":   err,
			}).Error("Failed to recover cooled key")
			continue
		}
		recoveredCount++
	}

	if recoveredCount > 0 {
		// 记录指标
		if p.metrics != nil {
			p.metrics.RecordKeyRecovery(groupID, recoveredCount)
		}

		logrus.WithFields(logrus.Fields{
			"groupID":   groupID,
			"recovered": recoveredCount,
			"total":     len(expiredKeys),
		}).Info("Recovered cooled keys")
	}

	return recoveredCount, nil
}

// ValidateKeys 验证密钥
func (p *MemoryLayeredPool) ValidateKeys(groupID uint, keyIDs []uint) error {
	if p.validator == nil {
		return NewPoolError(ErrorTypeConfiguration, "NO_VALIDATOR", "No validator configured")
	}

	// 获取分组信息
	var group models.Group
	if err := p.db.First(&group, groupID).Error; err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "GROUP_NOT_FOUND", "Failed to find group", err)
	}

	// 批量验证密钥
	var keys []models.APIKey
	if err := p.db.Where("id IN ? AND group_id = ?", keyIDs, groupID).Find(&keys).Error; err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "KEYS_NOT_FOUND", "Failed to find keys", err)
	}

	// 转换为指针切片
	keyPtrs := make([]*models.APIKey, len(keys))
	for i := range keys {
		keyPtrs[i] = &keys[i]
	}

	// 执行批量验证
	results := p.validator.ValidateBatch(keyPtrs, &group)

	// 处理验证结果
	invalidKeys := make([]uint, 0)
	for _, result := range results {
		if !result.Valid {
			invalidKeys = append(invalidKeys, result.KeyID)
		}
	}

	// 移除无效密钥
	if len(invalidKeys) > 0 {
		if err := p.RemoveKeys(groupID, invalidKeys); err != nil {
			logrus.WithFields(logrus.Fields{
				"groupID":     groupID,
				"invalidKeys": invalidKeys,
				"error":       err,
			}).Warn("Failed to remove invalid keys")
		}
	}

	logrus.WithFields(logrus.Fields{
		"groupID":    groupID,
		"totalKeys":  len(keyIDs),
		"validKeys":  len(keyIDs) - len(invalidKeys),
		"invalidKeys": len(invalidKeys),
	}).Info("Key validation completed")

	return nil
}

// GetPoolStats 获取池统计信息
func (p *MemoryLayeredPool) GetPoolStats(groupID uint) (*PoolStats, error) {
	stats := &PoolStats{
		GroupID:      groupID,
		PoolCounts:   make(map[PoolType]int),
		StatusCounts: make(map[KeyStatus]int),
		LastUpdated:  time.Now(),
	}

	// 获取各池的大小
	poolTypes := []PoolType{PoolTypeValidation, PoolTypeReady, PoolTypeActive, PoolTypeCooling}
	totalKeys := 0

	for _, poolType := range poolTypes {
		count, err := p.getPoolSize(groupID, poolType)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"groupID":  groupID,
				"poolType": poolType,
				"error":    err,
			}).Warn("Failed to get pool size")
			continue
		}

		stats.PoolCounts[poolType] = int(count)
		totalKeys += int(count)
	}

	stats.TotalKeys = totalKeys

	// 获取性能统计
	if p.metrics != nil {
		perfStats, err := p.metrics.GetMetrics(groupID)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"groupID": groupID,
				"error":   err,
			}).Warn("Failed to get performance metrics")
		} else {
			stats.Performance = perfStats
		}
	}

	return stats, nil
}

// GetStats 获取池统计信息（无需groupID参数）
func (p *MemoryLayeredPool) GetStats() (*PoolStats, error) {
	// 返回所有分组的聚合统计
	allStats := &PoolStats{
		GroupID:      0, // 表示聚合统计
		TotalKeys:    0,
		PoolCounts:   make(map[PoolType]int),
		StatusCounts: make(map[KeyStatus]int),
		LastUpdated:  time.Now(),
	}

	// 这里可以实现聚合逻辑，暂时返回空统计
	return allStats, nil
}

// initializeRecoveryComponents 初始化恢复组件
func (p *MemoryLayeredPool) initializeRecoveryComponents() error {
	// 创建恢复策略
	if p.recoveryStrategy == nil {
		p.recoveryStrategy = NewSmartRecoveryStrategy(nil)
	}

	// 创建恢复监控器
	if p.recoveryMonitor == nil {
		p.recoveryMonitor = NewRecoveryMonitor(nil)
	}

	// 创建恢复服务
	if p.recoveryService == nil {
		p.recoveryService = NewRecoveryService(
			p.db,
			p, // 传入自身作为LayeredKeyPool
			p.recoveryStrategy,
			nil, // 使用默认配置
		)
	}

	// 创建批量恢复处理器
	if p.batchProcessor == nil {
		calculator := NewDynamicRecoveryCalculator(nil)
		p.batchProcessor = NewBatchRecoveryProcessor(
			p.db,
			p, // 传入自身作为LayeredKeyPool
			calculator,
			nil, // 使用默认配置
		)
	}

	// 创建性能监控器
	if p.performanceMonitor == nil {
		p.performanceMonitor = NewPerformanceMonitor(nil)
	}

	return nil
}

// StartRecoveryServices 启动恢复服务
func (p *MemoryLayeredPool) StartRecoveryServices() error {
	if p.recoveryMonitor != nil {
		if err := p.recoveryMonitor.Start(); err != nil {
			return NewPoolErrorWithCause(ErrorTypeInternal, "MONITOR_START_FAILED", "Failed to start recovery monitor", err)
		}
	}

	if p.recoveryService != nil {
		if err := p.recoveryService.Start(); err != nil {
			return NewPoolErrorWithCause(ErrorTypeInternal, "SERVICE_START_FAILED", "Failed to start recovery service", err)
		}
	}

	if p.batchProcessor != nil {
		if err := p.batchProcessor.Start(); err != nil {
			return NewPoolErrorWithCause(ErrorTypeInternal, "BATCH_PROCESSOR_START_FAILED", "Failed to start batch processor", err)
		}
	}

	if p.performanceMonitor != nil {
		if err := p.performanceMonitor.Start(); err != nil {
			return NewPoolErrorWithCause(ErrorTypeInternal, "PERFORMANCE_MONITOR_START_FAILED", "Failed to start performance monitor", err)
		}
	}

	logrus.Info("Recovery services started for Memory layered pool")
	return nil
}

// StopRecoveryServices 停止恢复服务
func (p *MemoryLayeredPool) StopRecoveryServices() error {
	var errors []error

	if p.batchProcessor != nil {
		if err := p.batchProcessor.Stop(); err != nil {
			errors = append(errors, err)
		}
	}

	if p.recoveryService != nil {
		if err := p.recoveryService.Stop(); err != nil {
			errors = append(errors, err)
		}
	}

	if p.recoveryMonitor != nil {
		if err := p.recoveryMonitor.Stop(); err != nil {
			errors = append(errors, err)
		}
	}

	if p.performanceMonitor != nil {
		if err := p.performanceMonitor.Stop(); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to stop some recovery services: %v", errors)
	}

	logrus.Info("Recovery services stopped for Memory layered pool")
	return nil
}

// GetRecoveryMetrics 获取恢复指标
func (p *MemoryLayeredPool) GetRecoveryMetrics() *RecoveryMonitorMetrics {
	if p.recoveryMonitor != nil {
		return p.recoveryMonitor.GetMetrics()
	}
	return nil
}

// GetPerformanceMetrics 获取性能指标
func (p *MemoryLayeredPool) GetPerformanceMetrics() *PerformanceMetrics {
	if p.performanceMonitor != nil {
		return p.performanceMonitor.GetMetrics()
	}
	return nil
}

// GetPerformanceTimeSeries 获取性能时间序列数据
func (p *MemoryLayeredPool) GetPerformanceTimeSeries() *TimeSeriesData {
	if p.performanceMonitor != nil {
		return p.performanceMonitor.GetTimeSeries()
	}
	return nil
}

// TriggerManualRecovery 触发手动恢复
func (p *MemoryLayeredPool) TriggerManualRecovery(groupID uint, keyIDs []uint) error {
	if p.recoveryService == nil {
		return NewPoolError(ErrorTypeConfiguration, "NO_RECOVERY_SERVICE", "Recovery service not initialized")
	}

	// 创建恢复计划
	var plans []*RecoveryPlan
	for _, keyID := range keyIDs {
		// 获取密钥信息
		var key models.APIKey
		if err := p.db.First(&key, keyID).Error; err != nil {
			logrus.WithFields(logrus.Fields{"keyID": keyID, "error": err}).Warn("Failed to load key for manual recovery")
			continue
		}

		// 获取分组信息
		var group models.Group
		if err := p.db.First(&group, groupID).Error; err != nil {
			return NewPoolErrorWithCause(ErrorTypeStorage, "GROUP_NOT_FOUND", "Failed to find group", err)
		}

		// 创建恢复计划
		plan, err := p.recoveryStrategy.CreateRecoveryPlan(&key, &group, nil)
		if err != nil {
			logrus.WithFields(logrus.Fields{"keyID": keyID, "error": err}).Warn("Failed to create recovery plan")
			continue
		}

		// 立即调度
		plan.ScheduledAt = time.Now()
		plans = append(plans, plan)
	}

	if len(plans) == 0 {
		return NewPoolError(ErrorTypeValidation, "NO_VALID_PLANS", "No valid recovery plans created")
	}

	// 创建批次并提交
	batches, err := p.batchProcessor.CreateRecoveryBatches(plans)
	if err != nil {
		return NewPoolErrorWithCause(ErrorTypeInternal, "BATCH_CREATION_FAILED", "Failed to create recovery batches", err)
	}

	for _, batch := range batches {
		if err := p.batchProcessor.SubmitBatch(batch); err != nil {
			logrus.WithFields(logrus.Fields{"batchID": batch.ID, "error": err}).Warn("Failed to submit recovery batch")
		}
	}

	logrus.WithFields(logrus.Fields{
		"groupID":    groupID,
		"keyCount":   len(keyIDs),
		"planCount":  len(plans),
		"batchCount": len(batches),
	}).Info("Manual recovery triggered")

	return nil
}

// getRedisKey 生成内存存储键名（兼容Redis键名格式）
func (p *MemoryLayeredPool) getRedisKey(groupID uint, poolType PoolType) string {
	prefix := "memory_pool:"
	if p.memoryConfig != nil {
		// 使用配置的前缀，如果没有则使用默认值
		prefix = "memory_pool:"
	}
	return fmt.Sprintf("%sgroup:%d:%s", prefix, groupID, poolType)
}

// getKeyDetailsKey 生成密钥详情键名
func (p *MemoryLayeredPool) getKeyDetailsKey(keyID uint) string {
	prefix := "memory_pool:"
	if p.memoryConfig != nil {
		prefix = "memory_pool:"
	}
	return fmt.Sprintf("%skey:%d", prefix, keyID)
}

// syncKeyDetailsToRedis 同步密钥详情到内存存储
func (p *MemoryLayeredPool) syncKeyDetailsToRedis(keyID, groupID uint) error {
	var key models.APIKey
	if err := p.db.First(&key, keyID).Error; err != nil {
		return err
	}

	// 构建密钥详情
	details := map[string]interface{}{
		"id":         key.ID,
		"group_id":   key.GroupID,
		"key_value":  key.KeyValue,
		"status":     key.Status,
		"created_at": key.CreatedAt,
		"updated_at": key.UpdatedAt,
	}

	// 存储到分片内存存储
	detailsKey := p.getKeyDetailsKey(keyID)
	return p.shardedStore.HSet(detailsKey, details)
}

// maintenanceLoop 维护循环
func (p *MemoryLayeredPool) maintenanceLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.performMaintenance()
		}
	}
}

// performMaintenance 执行维护任务
func (p *MemoryLayeredPool) performMaintenance() {
	// 获取所有分组
	var groups []models.Group
	if err := p.db.Find(&groups).Error; err != nil {
		logrus.WithError(err).Error("Failed to load groups for maintenance")
		return
	}

	for _, group := range groups {
		// 恢复冷却的密钥
		if recovered, err := p.RecoverCooledKeys(group.ID); err != nil {
			logrus.WithFields(logrus.Fields{
				"groupID": group.ID,
				"error":   err,
			}).Error("Failed to recover cooled keys")
		} else if recovered > 0 {
			logrus.WithFields(logrus.Fields{
				"groupID":   group.ID,
				"recovered": recovered,
			}).Info("Recovered cooled keys")
		}

		// 补充池
		if err := p.RefillPools(group.ID); err != nil {
			logrus.WithFields(logrus.Fields{
				"groupID": group.ID,
				"error":   err,
			}).Error("Failed to refill pools")
		}
	}
}

// cacheCleanupLoop 缓存清理循环
func (p *MemoryLayeredPool) cacheCleanupLoop() {
	defer p.wg.Done()

	if p.localCache == nil {
		return
	}

	ticker := time.NewTicker(5 * time.Minute) // 每5分钟清理一次
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.cleanupExpiredCache()
		}
	}
}

// cleanupExpiredCache 清理过期缓存
func (p *MemoryLayeredPool) cleanupExpiredCache() {
	if p.localCache == nil {
		return
	}

	// 调用本地缓存的清理方法
	p.localCache.cleanupExpired()
}

// getCachedKey 从本地缓存获取密钥
func (p *MemoryLayeredPool) getCachedKey(groupID uint) *models.APIKey {
	if p.localCache == nil {
		return nil
	}

	// 这里简化实现，实际可能需要更复杂的缓存策略
	// 由于我们没有按groupID索引，这里返回nil
	// 实际实现中可能需要维护一个groupID到keyID的映射
	return nil
}

// setCachedKey 设置本地缓存中的密钥
func (p *MemoryLayeredPool) setCachedKey(keyID uint, key *models.APIKey) {
	if p.localCache == nil {
		return
	}

	// 使用localKeyCache的Set方法
	p.localCache.Set(keyID, key)
}

// removeCachedKey 从本地缓存移除密钥
func (p *MemoryLayeredPool) removeCachedKey(keyID uint) {
	if p.localCache == nil {
		return
	}

	p.localCache.Remove(keyID)
}

// getKeyDetails 获取密钥详情
func (p *MemoryLayeredPool) getKeyDetails(keyID uint) (map[string]string, error) {
	detailsKey := p.getKeyDetailsKey(keyID)
	return p.shardedStore.HGetAll(detailsKey)
}

// setKeyDetails 设置密钥详情
func (p *MemoryLayeredPool) setKeyDetails(keyID uint, details map[string]interface{}) error {
	detailsKey := p.getKeyDetailsKey(keyID)
	return p.shardedStore.HSet(detailsKey, details)
}

// buildAPIKeyFromDetails 从详情构建APIKey对象
func (p *MemoryLayeredPool) buildAPIKeyFromDetails(keyID, groupID uint, details map[string]string) (*models.APIKey, error) {
	keyValue, exists := details["key_value"]
	if !exists {
		return nil, NewPoolError(ErrorTypeValidation, "MISSING_KEY_VALUE", "Key details missing key_value")
	}

	status := details["status"]
	if status == "" {
		status = models.KeyStatusActive
	}

	// 解析数值字段
	requestCount, _ := strconv.ParseInt(details["request_count"], 10, 64)
	failureCount, _ := strconv.ParseInt(details["failure_count"], 10, 64)
	rateLimitCount, _ := strconv.ParseInt(details["rate_limit_count"], 10, 64)
	createdAt, _ := strconv.ParseInt(details["created_at"], 10, 64)

	apiKey := &models.APIKey{
		ID:             keyID,
		KeyValue:       keyValue,
		Status:         status,
		RequestCount:   requestCount,
		FailureCount:   failureCount,
		RateLimitCount: rateLimitCount,
		GroupID:        groupID,
		CreatedAt:      time.Unix(createdAt, 0),
	}

	// 解析可选的时间字段
	if lastUsedStr := details["last_used_at"]; lastUsedStr != "" {
		if lastUsed, err := strconv.ParseInt(lastUsedStr, 10, 64); err == nil {
			lastUsedTime := time.Unix(lastUsed, 0)
			apiKey.LastUsedAt = &lastUsedTime
		}
	}

	if last429Str := details["last_429_at"]; last429Str != "" {
		if last429, err := strconv.ParseInt(last429Str, 10, 64); err == nil {
			last429Time := time.Unix(last429, 0)
			apiKey.Last429At = &last429Time
		}
	}

	if resetAtStr := details["rate_limit_reset_at"]; resetAtStr != "" {
		if resetAt, err := strconv.ParseInt(resetAtStr, 10, 64); err == nil {
			resetAtTime := time.Unix(resetAt, 0)
			apiKey.RateLimitResetAt = &resetAtTime
		}
	}

	return apiKey, nil
}

// updateKeyStats 更新密钥统计
func (p *MemoryLayeredPool) updateKeyStats(keyID uint, success bool) {
	details, err := p.getKeyDetails(keyID)
	if err != nil {
		return
	}

	requestCount, _ := strconv.ParseInt(details["request_count"], 10, 64)
	updates := map[string]interface{}{
		"request_count": requestCount + 1,
		"last_used_at":  time.Now().Unix(),
	}

	if success {
		// 成功时重置失败计数
		updates["failure_count"] = 0
	}

	p.setKeyDetails(keyID, updates)
}

// handleKeyFailure 处理密钥失败
func (p *MemoryLayeredPool) handleKeyFailure(keyID, groupID uint) error {
	// 获取当前失败次数
	details, err := p.getKeyDetails(keyID)
	if err != nil {
		return err
	}

	failureCount, _ := strconv.ParseInt(details["failure_count"], 10, 64)
	newFailureCount := failureCount + 1

	// 获取分组配置
	var group models.Group
	if err := p.db.First(&group, groupID).Error; err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "GROUP_NOT_FOUND", "Failed to find group", err)
	}

	blacklistThreshold := group.EffectiveConfig.BlacklistThreshold
	updates := map[string]interface{}{
		"failure_count": newFailureCount,
	}

	// 检查是否需要拉黑
	if blacklistThreshold > 0 && newFailureCount >= int64(blacklistThreshold) {
		updates["status"] = models.KeyStatusInvalid

		// 从活跃池移除
		activeKey := p.getRedisKey(groupID, PoolTypeActive)
		if err := p.shardedStore.LRem(activeKey, 0, keyID); err != nil {
			logrus.WithFields(logrus.Fields{"keyID": keyID, "error": err}).Warn("Failed to remove invalid key from active pool")
		}

		logrus.WithFields(logrus.Fields{
			"keyID":              keyID,
			"groupID":            groupID,
			"failureCount":       newFailureCount,
			"blacklistThreshold": blacklistThreshold,
		}).Info("Key blacklisted due to excessive failures")
	} else {
		// 失败但未达到拉黑阈值，放回活跃池
		activeKey := p.getRedisKey(groupID, PoolTypeActive)
		if err := p.shardedStore.LPush(activeKey, keyID); err != nil {
			return NewPoolErrorWithCause(ErrorTypeStorage, "RETURN_FAILED", "Failed to return failed key to active pool", err)
		}
	}

	// 更新密钥详情
	if err := p.setKeyDetails(keyID, updates); err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "UPDATE_DETAILS_FAILED", "Failed to update key details", err)
	}

	return nil
}

// addToCoolingPool 添加密钥到冷却池
func (p *MemoryLayeredPool) addToCoolingPool(groupID uint, keyID uint, resetAt time.Time) error {
	coolingKey := p.getRedisKey(groupID, PoolTypeCooling)

	// 使用时间戳作为score
	score := float64(resetAt.Unix())

	// 使用ZADD操作添加到有序集合
	member := store.ZMember{
		Score:  score,
		Member: keyID,
	}
	return p.shardedStore.ZAdd(coolingKey, member)
}

// getExpiredFromCoolingPool 获取已过期的冷却密钥
func (p *MemoryLayeredPool) getExpiredFromCoolingPool(groupID uint) ([]uint, error) {
	coolingKey := p.getRedisKey(groupID, PoolTypeCooling)
	now := time.Now().Unix()

	// 获取score <= now的成员
	members, err := p.shardedStore.ZRangeByScore(coolingKey, 0, float64(now))
	if err != nil {
		return nil, err
	}

	expiredKeys := make([]uint, 0, len(members))
	for _, member := range members {
		if keyID, err := strconv.ParseUint(member, 10, 64); err == nil {
			expiredKeys = append(expiredKeys, uint(keyID))
		}
	}

	return expiredKeys, nil
}

// removeFromCoolingPool 从冷却池移除密钥
func (p *MemoryLayeredPool) removeFromCoolingPool(groupID uint, keyIDs []uint) error {
	coolingKey := p.getRedisKey(groupID, PoolTypeCooling)

	members := make([]interface{}, len(keyIDs))
	for i, keyID := range keyIDs {
		members[i] = keyID
	}

	return p.shardedStore.ZRem(coolingKey, members...)
}

// incrementRateLimitCount 增加429计数
func (p *MemoryLayeredPool) incrementRateLimitCount(details map[string]string) int64 {
	rateLimitCount, _ := strconv.ParseInt(details["rate_limit_count"], 10, 64)
	return rateLimitCount + 1
}

// recoverSingleCooledKey 恢复单个冷却密钥
func (p *MemoryLayeredPool) recoverSingleCooledKey(groupID, keyID uint) error {
	// 从冷却池移除
	if err := p.removeFromCoolingPool(groupID, []uint{keyID}); err != nil {
		return err
	}

	// 更新密钥状态
	updates := map[string]interface{}{
		"status":               models.KeyStatusActive,
		"rate_limit_reset_at":  nil,
	}
	if err := p.setKeyDetails(keyID, updates); err != nil {
		return err
	}

	// 添加到就绪池
	if err := p.addToReadyPool(groupID, []uint{keyID}); err != nil {
		return err
	}

	return nil
}

// getPoolSize 获取池大小
func (p *MemoryLayeredPool) getPoolSize(groupID uint, poolType PoolType) (int64, error) {
	switch poolType {
	case PoolTypeValidation:
		validationKey := p.getRedisKey(groupID, PoolTypeValidation)
		members, err := p.shardedStore.SMembers(validationKey)
		if err != nil {
			return 0, err
		}
		return int64(len(members)), nil

	case PoolTypeReady, PoolTypeActive:
		poolKey := p.getRedisKey(groupID, poolType)
		return p.shardedStore.LLen(poolKey)

	case PoolTypeCooling:
		coolingKey := p.getRedisKey(groupID, PoolTypeCooling)
		return p.shardedStore.ZCard(coolingKey)

	default:
		return 0, NewPoolError(ErrorTypeValidation, "UNKNOWN_POOL_TYPE", "Unknown pool type")
	}
}

// moveToActivePool 将密钥从就绪池移动到活跃池
func (p *MemoryLayeredPool) moveToActivePool(groupID uint, count int) ([]uint, error) {
	readyKey := p.getRedisKey(groupID, PoolTypeReady)
	activeKey := p.getRedisKey(groupID, PoolTypeActive)

	var movedKeys []uint

	for i := 0; i < count; i++ {
		// 从就绪池弹出一个密钥
		keyIDStr, err := p.shardedStore.Rotate(readyKey)
		if err != nil {
			if err == store.ErrNotFound {
				break // 就绪池为空
			}
			return movedKeys, err
		}

		keyID, err := strconv.ParseUint(keyIDStr, 10, 64)
		if err != nil {
			continue
		}

		// 添加到活跃池
		if err := p.shardedStore.LPush(activeKey, uint(keyID)); err != nil {
			return movedKeys, err
		}

		movedKeys = append(movedKeys, uint(keyID))
	}

	return movedKeys, nil
}

// addKeysToPool 向指定池添加密钥的内部方法
func (p *MemoryLayeredPool) addKeysToPool(groupID uint, keyIDs []uint, poolType PoolType) error {
	if len(keyIDs) == 0 {
		return nil
	}

	// 首先从数据库获取密钥详情
	var keys []models.APIKey
	if err := p.db.Where("id IN ? AND group_id = ?", keyIDs, groupID).Find(&keys).Error; err != nil {
		return NewPoolErrorWithCause(ErrorTypeStorage, "DB_QUERY_FAILED", "Failed to query keys from database", err)
	}

	if len(keys) == 0 {
		return NewPoolError(ErrorTypeValidation, "NO_KEYS_FOUND", "No valid keys found in database")
	}

	// 验证密钥状态
	validKeyIDs := make([]uint, 0, len(keys))
	for _, key := range keys {
		if key.Status == models.KeyStatusActive || key.Status == models.KeyStatusRateLimited {
			validKeyIDs = append(validKeyIDs, key.ID)
		}
	}

	if len(validKeyIDs) == 0 {
		return NewPoolError(ErrorTypeValidation, "NO_VALID_KEYS", "No valid keys to add to pool")
	}

	// 根据池类型执行不同的添加逻辑
	switch poolType {
	case PoolTypeValidation:
		return p.addToValidationPool(groupID, validKeyIDs)
	case PoolTypeReady:
		return p.addToReadyPool(groupID, validKeyIDs)
	case PoolTypeActive:
		return p.addToActivePool(groupID, validKeyIDs)
	case PoolTypeCooling:
		// 冷却池需要特殊处理，不应该直接添加
		return NewPoolError(ErrorTypeValidation, "INVALID_POOL_TYPE", "Cannot directly add keys to cooling pool")
	default:
		return NewPoolError(ErrorTypeValidation, "UNKNOWN_POOL_TYPE", "Unknown pool type")
	}
}

// addToReadyPool 添加密钥到就绪池
func (p *MemoryLayeredPool) addToReadyPool(groupID uint, keyIDs []uint) error {
	if len(keyIDs) == 0 {
		return nil
	}

	readyKey := p.getRedisKey(groupID, PoolTypeReady)

	// 批量添加到就绪池
	for _, keyID := range keyIDs {
		if err := p.shardedStore.LPush(readyKey, keyID); err != nil {
			return NewPoolErrorWithCause(ErrorTypeStorage, "LPUSH_FAILED", "Failed to add key to ready pool", err)
		}
	}

	// 更新密钥详情到内存存储
	for _, keyID := range keyIDs {
		if err := p.syncKeyDetailsToRedis(keyID, groupID); err != nil {
			logrus.WithFields(logrus.Fields{"keyID": keyID, "error": err}).Warn("Failed to sync key details to memory store")
		}
	}

	// 发送事件
	if p.eventHandler != nil {
		event := &KeyPoolEvent{
			Type:      EventPoolRefilled,
			GroupID:   groupID,
			PoolType:  PoolTypeReady,
			Message:   fmt.Sprintf("Added %d keys to ready pool", len(keyIDs)),
			Timestamp: time.Now(),
			Metadata:  keyIDs,
		}
		p.eventHandler.HandleEvent(event)
	}

	logrus.WithFields(logrus.Fields{
		"groupID": groupID,
		"count":   len(keyIDs),
		"pool":    PoolTypeReady,
	}).Info("Keys added to ready pool")

	return nil
}

// addToValidationPool 添加密钥到验证池
func (p *MemoryLayeredPool) addToValidationPool(groupID uint, keyIDs []uint) error {
	if len(keyIDs) == 0 {
		return nil
	}

	validationKey := p.getRedisKey(groupID, PoolTypeValidation)

	// 转换为interface{}切片
	members := make([]interface{}, len(keyIDs))
	for i, keyID := range keyIDs {
		members[i] = keyID
	}

	return p.shardedStore.SAdd(validationKey, members...)
}

// addToActivePool 添加密钥到活跃池
func (p *MemoryLayeredPool) addToActivePool(groupID uint, keyIDs []uint) error {
	if len(keyIDs) == 0 {
		return nil
	}

	activeKey := p.getRedisKey(groupID, PoolTypeActive)

	// 批量添加到活跃池
	for _, keyID := range keyIDs {
		if err := p.shardedStore.LPush(activeKey, keyID); err != nil {
			return NewPoolErrorWithCause(ErrorTypeStorage, "LPUSH_FAILED", "Failed to add key to active pool", err)
		}
	}

	// 更新密钥详情到内存存储
	for _, keyID := range keyIDs {
		if err := p.syncKeyDetailsToRedis(keyID, groupID); err != nil {
			logrus.WithFields(logrus.Fields{"keyID": keyID, "error": err}).Warn("Failed to sync key details to memory store")
		}
	}

	// 发送事件
	if p.eventHandler != nil {
		event := &KeyPoolEvent{
			Type:      EventPoolRefilled,
			GroupID:   groupID,
			PoolType:  PoolTypeActive,
			Message:   fmt.Sprintf("Added %d keys to active pool", len(keyIDs)),
			Timestamp: time.Now(),
			Metadata:  keyIDs,
		}
		p.eventHandler.HandleEvent(event)
	}

	logrus.WithFields(logrus.Fields{
		"groupID": groupID,
		"count":   len(keyIDs),
		"pool":    PoolTypeActive,
	}).Info("Keys added to active pool")

	return nil
}

// Set 设置缓存条目
func (c *localKeyCache) Set(keyID uint, key *models.APIKey) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[keyID] = &cacheEntry{
		key:       key,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// Get 获取缓存条目
func (c *localKeyCache) Get(keyID uint) (*models.APIKey, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.cache[keyID]
	if !exists {
		return nil, false
	}

	// 检查是否过期
	if time.Now().After(entry.expiresAt) {
		// 过期了，删除并返回nil
		delete(c.cache, keyID)
		return nil, false
	}

	return entry.key, true
}

// Remove 从缓存中移除指定的密钥
func (c *localKeyCache) Remove(keyID uint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, keyID)
}

// cleanupExpired 清理过期的缓存条目
func (c *localKeyCache) cleanupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for keyID, entry := range c.cache {
		if now.After(entry.expiresAt) {
			delete(c.cache, keyID)
		}
	}
}

// UpdateConfig 更新本地缓存配置
func (c *localKeyCache) UpdateConfig(config *LocalCacheConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 更新配置
	c.maxSize = config.MaxSize
	c.ttl = config.TTL

	// 如果新的最大大小小于当前缓存大小，需要清理一些条目
	if len(c.cache) > c.maxSize {
		// 简单的清理策略：清理最旧的条目
		now := time.Now()
		keysToRemove := make([]uint, 0)

		for keyID, entry := range c.cache {
			if len(keysToRemove) >= len(c.cache)-c.maxSize {
				break
			}
			// 优先清理过期的条目
			if now.After(entry.expiresAt) {
				keysToRemove = append(keysToRemove, keyID)
			}
		}

		// 如果过期条目不够，清理一些最旧的条目
		if len(keysToRemove) < len(c.cache)-c.maxSize {
			for keyID := range c.cache {
				if len(keysToRemove) >= len(c.cache)-c.maxSize {
					break
				}
				found := false
				for _, removeID := range keysToRemove {
					if removeID == keyID {
						found = true
						break
					}
				}
				if !found {
					keysToRemove = append(keysToRemove, keyID)
				}
			}
		}

		// 执行清理
		for _, keyID := range keysToRemove {
			delete(c.cache, keyID)
		}
	}

	return nil
}

// GetKeyStatus 获取密钥状态
func (p *MemoryLayeredPool) GetKeyStatus(keyID uint) (KeyStatus, error) {
	// 获取密钥详情
	details, err := p.getKeyDetails(keyID)
	if err != nil {
		return "", NewPoolErrorWithCause(ErrorTypeStorage, "KEY_DETAILS_FAILED", "Failed to get key details", err)
	}

	// 从详情中获取状态
	if statusStr, exists := details["status"]; exists {
		return KeyStatus(statusStr), nil
	}

	// 如果没有状态信息，检查密钥在哪个池中
	groupIDStr, exists := details["group_id"]
	if !exists {
		return "", NewPoolError(ErrorTypeValidation, "MISSING_GROUP_ID", "Key details missing group_id")
	}

	groupID, err := strconv.ParseUint(groupIDStr, 10, 64)
	if err != nil {
		return "", NewPoolErrorWithCause(ErrorTypeInternal, "INVALID_GROUP_ID", "Invalid group ID format", err)
	}

	// 检查密钥在哪个池中
	poolTypes := []PoolType{PoolTypeValidation, PoolTypeReady, PoolTypeActive, PoolTypeCooling}
	for _, poolType := range poolTypes {
		keys, err := p.ListKeys(uint(groupID), poolType)
		if err != nil {
			continue
		}

		for _, id := range keys {
			if id == keyID {
				// 根据池类型推断状态
				switch poolType {
				case PoolTypeValidation:
					return models.KeyStatusActive, nil // 验证中的密钥视为活跃
				case PoolTypeReady:
					return models.KeyStatusActive, nil // 就绪的密钥视为活跃
				case PoolTypeActive:
					return models.KeyStatusActive, nil // 活跃的密钥
				case PoolTypeCooling:
					return models.KeyStatusRateLimited, nil // 冷却中的密钥视为受限
				}
			}
		}
	}

	// 如果在所有池中都找不到，可能是无效密钥
	return models.KeyStatusInvalid, nil
}

// ListKeys 列出指定池中的密钥
func (p *MemoryLayeredPool) ListKeys(groupID uint, poolType PoolType) ([]uint, error) {
	switch poolType {
	case PoolTypeValidation:
		return p.listValidationKeys(groupID)
	case PoolTypeReady:
		return p.listReadyKeys(groupID)
	case PoolTypeActive:
		return p.listActiveKeys(groupID)
	case PoolTypeCooling:
		return p.listCoolingKeys(groupID)
	default:
		return nil, NewPoolError(ErrorTypeValidation, "UNKNOWN_POOL_TYPE", "Unknown pool type")
	}
}

// listValidationKeys 列出验证池中的密钥
func (p *MemoryLayeredPool) listValidationKeys(groupID uint) ([]uint, error) {
	validationKey := p.getRedisKey(groupID, PoolTypeValidation)
	members, err := p.shardedStore.SMembers(validationKey)
	if err != nil {
		return nil, NewPoolErrorWithCause(ErrorTypeStorage, "SMEMBERS_FAILED", "Failed to list validation keys", err)
	}

	keyIDs := make([]uint, 0, len(members))
	for _, member := range members {
		if keyID, err := strconv.ParseUint(member, 10, 64); err == nil {
			keyIDs = append(keyIDs, uint(keyID))
		}
	}

	return keyIDs, nil
}

// listReadyKeys 列出就绪池中的密钥
func (p *MemoryLayeredPool) listReadyKeys(groupID uint) ([]uint, error) {
	readyKey := p.getRedisKey(groupID, PoolTypeReady)

	// 获取列表中的所有元素
	list, err := p.shardedStore.LRange(readyKey, 0, -1)
	if err != nil {
		return nil, NewPoolErrorWithCause(ErrorTypeStorage, "LRANGE_FAILED", "Failed to list ready keys", err)
	}

	keyIDs := make([]uint, 0, len(list))
	for _, item := range list {
		if keyID, err := strconv.ParseUint(item, 10, 64); err == nil {
			keyIDs = append(keyIDs, uint(keyID))
		}
	}

	return keyIDs, nil
}

// listActiveKeys 列出活跃池中的密钥
func (p *MemoryLayeredPool) listActiveKeys(groupID uint) ([]uint, error) {
	activeKey := p.getRedisKey(groupID, PoolTypeActive)

	// 获取列表中的所有元素
	list, err := p.shardedStore.LRange(activeKey, 0, -1)
	if err != nil {
		return nil, NewPoolErrorWithCause(ErrorTypeStorage, "LRANGE_FAILED", "Failed to list active keys", err)
	}

	keyIDs := make([]uint, 0, len(list))
	for _, item := range list {
		if keyID, err := strconv.ParseUint(item, 10, 64); err == nil {
			keyIDs = append(keyIDs, uint(keyID))
		}
	}

	return keyIDs, nil
}

// listCoolingKeys 列出冷却池中的密钥
func (p *MemoryLayeredPool) listCoolingKeys(groupID uint) ([]uint, error) {
	coolingKey := p.getRedisKey(groupID, PoolTypeCooling)

	// 获取有序集合中的所有元素
	members, err := p.shardedStore.ZRange(coolingKey, 0, -1)
	if err != nil {
		return nil, NewPoolErrorWithCause(ErrorTypeStorage, "ZRANGE_FAILED", "Failed to list cooling keys", err)
	}

	keyIDs := make([]uint, 0, len(members))
	for _, member := range members {
		if keyID, err := strconv.ParseUint(member, 10, 64); err == nil {
			keyIDs = append(keyIDs, uint(keyID))
		}
	}

	return keyIDs, nil
}
