#!/usr/bin/env node

// 简单的TypeScript类型检查脚本
const { execSync } = require('child_process');

try {
  console.log('开始TypeScript类型检查...');
  execSync('npx vue-tsc --noEmit', { stdio: 'inherit' });
  console.log('✅ TypeScript类型检查通过！');
  process.exit(0);
} catch (error) {
  console.error('❌ TypeScript类型检查失败');
  process.exit(1);
}
