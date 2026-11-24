<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <section v-if="meta.key === 'snapshot_mgr'" class="recovery-panel" data-testid="snapshot-recovery-panel">
      <h3>快照恢复数据包（异地恢复离线载体）</h3>
      <p class="page-desc">
        导出指定版本快照的分区数据，包含来源表、快照版本、分区对象及 SHA-256 内容摘要，
        同一版本重复导出内容一致，可交异地作业离线恢复。
      </p>
      <form class="filter-bar" @submit.prevent="handleBuildPackage">
        <label class="filter-item">
          <span>快照行ID</span>
          <input v-model="packageForm.snapshotRowId" data-testid="package-row-id" required />
        </label>
        <label class="filter-item">
          <span>快照版本</span>
          <input v-model="packageForm.snapshotVersion" data-testid="package-version" required />
        </label>
        <button class="btn primary" type="submit" data-testid="build-package">生成数据包</button>
        <button class="btn" type="button" data-testid="download-package" @click="handleDownloadPackage">
          下载离线载体
        </button>
      </form>
      <table v-if="recoveryPackages.length" class="data-table">
        <thead>
          <tr><th>包摘要</th><th>来源表</th><th>快照</th><th>版本</th><th>分区对象</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in recoveryPackages" :key="item.package_digest">
            <td>{{ item.package_digest.slice(0, 12) }}…</td>
            <td>{{ item.source_table }}</td>
            <td>{{ item.snapshot_id }}</td>
            <td>{{ item.snapshot_version }}</td>
            <td>{{ item.partitions.join('、') }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section v-if="meta.key === 'table_catalog'" class="recovery-panel" data-testid="catalog-recovery-panel">
      <h3>异地快照恢复（带回执）</h3>
      <p class="page-desc">
        表目录同步读取恢复数据包，从目标恢复作业创建可核验分区。逐分区发布核验通过即生效；
        整批确认需全部预期分区核验通过后一起发布。摘要不符的对象不可读、不记成功，可补送修复。
      </p>
      <form class="filter-bar" @submit.prevent="handleStartRecovery">
        <label class="filter-item">
          <span>同步记录ID</span>
          <input v-model="recoveryForm.catalogRowId" data-testid="recovery-row-id" required />
        </label>
        <label class="filter-item">
          <span>数据包摘要</span>
          <input v-model="recoveryForm.packageDigest" data-testid="recovery-package-digest" required />
        </label>
        <label class="filter-item">
          <span>原有无关分区</span>
          <input v-model="recoveryForm.preexisting" data-testid="recovery-preexisting" placeholder="如 p3" />
        </label>
        <label class="filter-item">
          <span>发布边界</span>
          <select v-model="recoveryForm.boundary" data-testid="recovery-boundary">
            <option value="partition">逐分区发布</option>
            <option value="batch">整批确认</option>
          </select>
        </label>
        <button class="btn primary" type="submit" data-testid="start-recovery">发起异地恢复</button>
        <button class="btn" type="button" data-testid="load-partitions" @click="handleLoadPartitions">
          重开目标表核对
        </button>
      </form>
      <div v-if="lastReceipt" class="receipt-card" data-testid="recovery-receipt">
        <strong>回执：{{ lastReceipt.ok ? '全部生效' : '存在待补齐对象' }}</strong>
        <p>发布边界：{{ lastReceipt.publish_boundary }} ｜ 作业：{{ lastReceipt.job_id }} ｜ 状态：{{ lastReceipt.job_status }}</p>
        <p>已生效：{{ (lastReceipt.effective_partitions || []).join('、') || '无' }}</p>
        <p>待补：{{ (lastReceipt.pending_partitions || []).join('、') || '无' }}</p>
        <p v-if="lastReceipt.note" class="error-text">{{ lastReceipt.note }}</p>
      </div>
      <table v-if="readablePartitions.length" class="data-table">
        <thead>
          <tr><th>可读分区</th><th>来源</th><th>快照</th><th>已发布</th></tr>
        </thead>
        <tbody>
          <tr v-for="part in readablePartitions" :key="part.partition">
            <td>{{ part.partition }}</td>
            <td>{{ part.source }}</td>
            <td>{{ part.snapshot_id }}</td>
            <td>{{ part.published ? '是' : '否' }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import {
  buildRecoveryPackage,
  createEntry,
  downloadEntries,
  downloadRecoveryPackage,
  getReadablePartitions,
  listEntries,
  listRecoveryPackages,
  moduleMeta,
  runAction as applyAction,
  startDisasterRecovery,
} from '@/api/backend-service'
import type {
  RecoveryPackageSummary,
  RecoveryReceipt,
  ReadablePartition,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

// ---------------------------------------------------------------------------
// 快照恢复数据包 / 异地恢复面板
// ---------------------------------------------------------------------------

const packageForm = reactive({ snapshotRowId: '1', snapshotVersion: 'v1' })
const recoveryPackages = ref<RecoveryPackageSummary[]>([])
const recoveryForm = reactive({
  catalogRowId: '1',
  packageDigest: '',
  preexisting: 'p3',
  boundary: 'partition',
})
const lastReceipt = ref<RecoveryReceipt | null>(null)
const readablePartitions = ref<ReadablePartition[]>([])

async function refreshPackages() {
  try {
    const payload = await listRecoveryPackages()
    recoveryPackages.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据包读取失败'
  }
}

async function handleBuildPackage() {
  errorMessage.value = ''
  try {
    await buildRecoveryPackage(
      Number(packageForm.snapshotRowId),
      packageForm.snapshotVersion,
    )
    await refreshPackages()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据包生成失败'
  }
}

async function handleDownloadPackage() {
  errorMessage.value = ''
  try {
    await downloadRecoveryPackage(
      Number(packageForm.snapshotRowId),
      packageForm.snapshotVersion,
    )
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据包下载失败'
  }
}

async function handleStartRecovery() {
  errorMessage.value = ''
  try {
    const preexisting = recoveryForm.preexisting
      .split(/[，,、\s]+/)
      .map((item) => item.trim())
      .filter(Boolean)
    lastReceipt.value = await startDisasterRecovery(
      Number(recoveryForm.catalogRowId),
      {
        publish_boundary: recoveryForm.boundary,
        package_digest: recoveryForm.packageDigest,
        preexisting_partitions: preexisting,
      },
    )
    readablePartitions.value = lastReceipt.value.readable_partitions ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '异地恢复发起失败'
  }
}

async function handleLoadPartitions() {
  errorMessage.value = ''
  try {
    const view = await getReadablePartitions(Number(recoveryForm.catalogRowId))
    readablePartitions.value = view.partitions
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '可读分区读取失败'
  }
}

onMounted(() => {
  void reload()
  if (meta.key === 'snapshot_mgr') void refreshPackages()
})
</script>
