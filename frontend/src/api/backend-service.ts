import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---------------------------------------------------------------------------
// 快照恢复数据包与异地恢复
// ---------------------------------------------------------------------------

export type RecoveryPackageSummary = {
  package_digest: string
  source_table: string
  snapshot_id: string
  snapshot_version: string
  partitions: string[]
}

export type ReadablePartition = {
  partition: string
  source: string
  snapshot_id: string
  digest?: string
  published: boolean
}

export type RecoveryReceipt = {
  ok: boolean
  job_id?: string
  phase?: string
  catalog_id?: string
  target_table?: string
  source_table?: string
  snapshot_id?: string
  snapshot_version?: string
  publish_boundary?: string
  package_digest?: string
  expected_partitions?: string[]
  effective_partitions: string[]
  pending_partitions: string[]
  object_states?: Record<string, { state: string; published: boolean; write_count?: number }>
  job_status?: string
  note?: string
  message?: string
  duplicate?: boolean
  written?: boolean
  readable_partitions?: ReadablePartition[]
}

export async function buildRecoveryPackage(
  snapshotId: number,
  snapshotVersion: string,
  partitions: { partition: string; payload: unknown }[] = [],
): Promise<{ ok: boolean; package_digest: string; manifest: Record<string, unknown>; object_count: number }> {
  const response = await request(`/api/recovery/snapshots/${snapshotId}/packages`, {
    method: 'POST',
    body: JSON.stringify({ snapshot_version: snapshotVersion, partitions }),
  })
  if (!response.ok) throw new Error('恢复数据包构建失败')
  return await response.json()
}

export async function downloadRecoveryPackage(
  snapshotId: number,
  snapshotVersion: string,
): Promise<void> {
  const query = new URLSearchParams({ snapshot_version: snapshotVersion }).toString()
  const response = await request(`/api/recovery/snapshots/${snapshotId}/packages/download?${query}`)
  if (!response.ok) throw new Error('恢复数据包下载失败')
  const blob = await response.blob()
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `recovery_package_snapshot-${snapshotId}_${snapshotVersion}.json`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function listRecoveryPackages(): Promise<{ ok: boolean; items: RecoveryPackageSummary[] }> {
  const response = await request('/api/recovery/packages')
  if (!response.ok) throw new Error('恢复数据包列表读取失败')
  return await response.json()
}

export async function startDisasterRecovery(
  catalogId: number,
  payload: {
    publish_boundary: string
    package?: Record<string, unknown> | null
    package_digest?: string | null
    preexisting_partitions?: string[]
  },
): Promise<RecoveryReceipt> {
  const response = await request(`/api/recovery/catalogs/${catalogId}/jobs`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  if (!response.ok) throw new Error('异地恢复作业发起失败')
  return await response.json()
}

export async function resendRecoveryObject(
  catalogId: number,
  payload: { job_id: string; partition: string; obj: Record<string, unknown> },
): Promise<RecoveryReceipt> {
  const response = await request(`/api/recovery/catalogs/${catalogId}/jobs/resend`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  if (!response.ok) throw new Error('补送恢复对象失败')
  return await response.json()
}

export async function listRecoveryJobs(
  catalogId: number,
): Promise<{ ok: boolean; items: RecoveryReceipt[] }> {
  const response = await request(`/api/recovery/catalogs/${catalogId}/jobs`)
  if (!response.ok) throw new Error('恢复作业读取失败')
  return await response.json()
}

export async function getReadablePartitions(
  catalogId: number,
): Promise<{ ok: boolean; catalog_id: string; target_table: string; partitions: ReadablePartition[] }> {
  const response = await request(`/api/recovery/catalogs/${catalogId}/partitions`)
  if (!response.ok) throw new Error('可读分区读取失败')
  return await response.json()
}
