<template>
  <div class="dataset-metadata-panel space-y-8 animate-fade-in">
    <!-- Loading State -->
    <div v-if="loading" class="space-y-6">
      <div class="card grid grid-cols-2 md:grid-cols-4 gap-4 p-6">
        <el-skeleton v-for="i in 4" :key="i" animated>
          <template #template>
            <el-skeleton-item variant="rect" class="w-full h-24 rounded-xl" />
          </template>
        </el-skeleton>
      </div>
      <div class="card p-6">
        <el-skeleton animated :rows="5" />
      </div>
      <div class="card p-6">
        <el-skeleton animated :rows="8" />
      </div>
    </div>

   <!-- Error State -->
    <div v-else-if="error" class="card bg-red-50/50 dark:bg-red-900/10 border-red-200/50 dark:border-red-800/50 p-8 shadow-sm">
      <div class="flex flex-col items-center text-center gap-4">
        <div class="i-carbon-warning-alt text-5xl text-red-500 animate-pulse" />
        <div>
          <h3 class="font-bold text-xl text-red-900 dark:text-red-100 mb-2">Metadata Extraction Failed</h3>
          <p class="text-sm text-red-700 dark:text-red-300 max-w-md mx-auto">{{ error }}</p>
        </div>
        <el-button type="danger" plain round @click="loadMetadata">
          <div class="i-carbon-renew mr-2" /> Try Again
        </el-button>
      </div>
    </div>

    <!-- Metadata Content -->
    <transition name="fade" mode="out-in">
      <div v-if="metadata && !loading" class="space-y-6">
        <!-- Frozen Status Banner -->
        <div v-if="metadata.is_frozen" class="mb-6 p-6 bg-indigo-50/80 dark:bg-indigo-900/10 border border-indigo-200 dark:border-indigo-800 rounded-3xl shadow-sm flex items-center justify-between">
           <div class="flex items-center gap-4">
              <div class="p-3 bg-indigo-500 text-white rounded-2xl shadow-lg shadow-indigo-500/20">
                 <div class="i-carbon-locked text-3xl" />
              </div>
              <div>
                 <h2 class="text-lg font-black text-indigo-900 dark:text-indigo-100">Dataset Version Frozen</h2>
                 <p class="text-xs text-indigo-600 dark:text-indigo-400">This snapshot is locked for production use. Hash: <span class="font-mono text-[10px]">{{ metadata.signature }}</span></p>
              </div>
           </div>
           <el-tag type="info" effect="dark" round class="px-4">Snapshot #{{ metadata.snapshot_id }}</el-tag>
        </div>
        <!-- Overview Section -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
          <!-- Rows Stat -->
          <div class="stat-box bg-blue-50/80 dark:bg-blue-900/10 border border-blue-100 dark:border-blue-900/30 rounded-2xl p-5 shadow-sm hover:shadow-md transition-all group">
            <div class="flex justify-between items-start mb-2">
              <div class="p-2 bg-blue-500 text-white rounded-lg shadow-blue-500/20 shadow-lg group-hover:scale-110 transition-transform">
                <div class="i-carbon-rows text-xl" />
              </div>
              <span class="text-[10px] font-bold text-blue-400 uppercase tracking-widest">Dataset Size</span>
            </div>
            <div class="text-2xl font-black text-gray-900 dark:text-white mt-4">
              {{ formatNumber(metadata.statistics.total_rows || 0) }}
            </div>
            <div class="text-xs text-blue-600 dark:text-blue-400 font-medium">Total Samples</div>
          </div>

          <!-- Features Stat -->
          <div class="stat-box bg-emerald-50/80 dark:bg-emerald-900/10 border border-emerald-100 dark:border-emerald-900/30 rounded-2xl p-5 shadow-sm hover:shadow-md transition-all group">
            <div class="flex justify-between items-start mb-2">
              <div class="p-2 bg-emerald-500 text-white rounded-lg shadow-emerald-500/20 shadow-lg group-hover:scale-110 transition-transform">
                <div class="i-carbon-list-boxes text-xl" />
              </div>
              <span class="text-[10px] font-bold text-emerald-400 uppercase tracking-widest">Dimensions</span>
            </div>
            <div class="text-2xl font-black text-gray-900 dark:text-white mt-4">
              {{ metadata.statistics.num_features || 0 }}
            </div>
            <div class="text-xs text-emerald-600 dark:text-emerald-400 font-medium">Features Schema</div>
          </div>

          <!-- Splits Stat -->
          <div class="stat-box bg-purple-50/80 dark:bg-purple-900/10 border border-purple-100 dark:border-purple-900/30 rounded-2xl p-5 shadow-sm hover:shadow-md transition-all group">
            <div class="flex justify-between items-start mb-2">
              <div class="p-2 bg-purple-500 text-white rounded-lg shadow-purple-500/20 shadow-lg group-hover:scale-110 transition-transform">
                <div class="i-carbon-data-table text-xl" />
              </div>
              <span class="text-[10px] font-bold text-purple-400 uppercase tracking-widest">Organization</span>
            </div>
            <div class="text-2xl font-black text-gray-900 dark:text-white mt-4">
              {{ metadata.statistics.num_splits || 0 }}
            </div>
            <div class="text-xs text-purple-600 dark:text-purple-400 font-medium">Data Splits</div>
          </div>

          <!-- Size Stat -->
          <div class="stat-box bg-amber-50/80 dark:bg-amber-900/10 border border-amber-100 dark:border-amber-900/30 rounded-2xl p-5 shadow-sm hover:shadow-md transition-all group">
            <div class="flex justify-between items-start mb-2">
              <div class="p-2 bg-amber-500 text-white rounded-lg shadow-amber-500/20 shadow-lg group-hover:scale-110 transition-transform">
                <div class="i-carbon-save-series text-xl" />
              </div>
              <span class="text-[10px] font-bold text-amber-400 uppercase tracking-widest">Storage</span>
            </div>
            <div class="text-2xl font-black text-gray-900 dark:text-white mt-4">
              {{ formatSize(metadata.statistics.total_bytes || 0) }}
            </div>
            <div class="text-xs text-amber-600 dark:text-amber-400 font-medium">Estimated Disk Size</div>
          </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <!-- Features Schema Table -->
          <div class="card col-span-2 overflow-hidden border border-gray-200 dark:border-gray-800 shadow-sm">
            <div class="p-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-gray-50/50 dark:bg-gray-900/50">
              <h3 class="font-bold flex items-center gap-2 text-gray-800 dark:text-gray-100 uppercase tracking-tighter">
                <div class="i-carbon-list-boxes text-blue-500" />
                Feature Definitions & Quality
              </h3>
              <div class="flex gap-2">
                <el-button v-if="!metadata.is_frozen" size="small" type="warning" plain round @click="freezeDataset">
                  <div class="i-carbon-locked mr-1" /> Freeze Revision
                </el-button>
                <el-tag v-if="metadata.compliance_status === 'flagged'" size="small" type="danger" effect="dark" round>
                  <div class="i-carbon-warning-alt mr-1 inline-block" /> Flagged
                </el-tag>
                <el-tag size="small" type="info" effect="dark" round>{{ Object.keys(metadata.features).length }} Features</el-tag>
              </div>
            </div>
            
            <div class="overflow-x-auto min-h-[300px]">
              <table class="w-full text-sm">
                <thead class="bg-gray-50/50 dark:bg-gray-800/50 text-[10px] uppercase text-gray-400 tracking-wider">
                  <tr>
                    <th class="px-6 py-4 text-left font-bold">Field</th>
                    <th class="px-6 py-4 text-left font-bold">Type</th>
                    <th class="px-6 py-4 text-left font-bold">Quality Metrics</th>
                    <th class="px-6 py-4 text-left font-bold">Actions</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-gray-100 dark:divide-gray-800">
                  <tr v-for="(feature, name) in metadata.features" :key="name" class="hover:bg-gray-50/80 dark:hover:bg-gray-800/40 transition-colors group">
                    <td class="px-6 py-4">
                      <div class="font-mono text-blue-600 dark:text-blue-400 font-semibold text-xs group-hover:translate-x-1 transition-transform inline-block">
                        {{ name }}
                      </div>
                    </td>
                    <td class="px-6 py-4">
                      <span :class="[
                        'inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[10px] font-bold uppercase tracking-tight shadow-sm',
                        getTypeColor(feature.type)
                      ]">
                        <div :class="getTypeIcon(feature.type)" />
                        {{ feature.type }}
                      </span>
                    </td>
                    <td class="px-6 py-4">
                       <div class="flex flex-wrap gap-1.5">
                          <el-tooltip v-if="getQualityLabel(name)" :content="getQualityContent(name)" placement="top">
                             <el-tag size="small" :type="getQualityType(name)" effect="plain" class="cursor-help">
                                <div :class="getQualityIcon(name)" class="mr-1 inline-block" />
                                {{ getQualityLabel(name) }}
                             </el-tag>
                          </el-tooltip>
                          <span v-else class="text-[10px] text-gray-400 font-mono italic">No anomalies detected</span>
                       </div>
                    </td>
                    <td class="px-6 py-4">
                      <div class="flex items-center gap-2">
                        <el-button size="small" circle plain @click="showFeatureStats(name)">
                          <div class="i-carbon-chart-multitype text-blue-500" />
                        </el-button>
                        <el-tooltip v-if="splitStats?.[name]?.schema_warnings?.length" :content="splitStats[name].schema_warnings.join(' | ')" placement="top">
                           <div class="i-carbon-warning-alt text-amber-500 cursor-help" />
                        </el-tooltip>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <!-- Right Column: Distribution & Splits -->
          <div class="space-y-6">
            <!-- Type Distribution Chart-like cards -->
            <div class="card overflow-hidden">
              <h3 class="p-5 font-bold flex items-center gap-2 text-gray-800 dark:text-gray-200 border-b border-gray-100 dark:border-gray-800 text-sm uppercase tracking-tighter">
                <div class="i-carbon-chart-pie text-emerald-500" />
                Type Distribution
              </h3>
              <div class="p-5 space-y-3">
                <div v-for="(count, type) in metadata.type_distribution" :key="type" class="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-900 rounded-xl border border-gray-100 dark:border-gray-800 transition-all hover:border-emerald-500/30">
                  <div class="flex items-center gap-3">
                    <div class="p-2 rounded-lg bg-white dark:bg-gray-800 shadow-sm border border-gray-100 dark:border-gray-700">
                      <div :class="getTypeIcon(type)" class="text-lg" :style="{color: getTypeIconColor(type)}" />
                    </div>
                    <span class="text-xs font-bold uppercase text-gray-500 dark:text-gray-400">{{ type }}</span>
                  </div>
                  <div class="flex flex-col items-end">
                    <span class="text-lg font-black text-gray-900 dark:text-white leading-none">{{ count }}</span>
                    <span class="text-[9px] text-gray-400 mt-1 uppercase mt-0.5">{{ ((count / metadata.statistics.num_features) * 100).toFixed(0) }}%</span>
                  </div>
                </div>
              </div>
            </div>

            <!-- Splits Information Card -->
            <div class="card overflow-hidden">
               <h3 class="p-5 font-bold flex items-center gap-2 text-gray-800 dark:text-gray-200 border-b border-gray-100 dark:border-gray-800 text-sm uppercase tracking-tighter">
                <div class="i-carbon-data-table text-purple-500" />
                Data Splits
              </h3>
              <div class="divide-y divide-gray-100 dark:divide-gray-800">
                <div v-for="(split, name) in metadata.splits" :key="name" class="p-4 hover:bg-gray-50 dark:hover:bg-gray-900 transition-colors">
                  <div class="flex justify-between items-center mb-1">
                    <span class="font-bold text-sm text-gray-900 dark:text-gray-100">{{ name }}</span>
                    <span class="text-[10px] font-mono p-1 px-2 bg-gray-100 dark:bg-gray-800 rounded">{{ formatSize(split.num_bytes) }}</span>
                  </div>
                  <div class="flex justify-between items-end">
                    <div class="text-xs text-gray-500 font-mono">{{ formatNumber(split.num_rows) }} samples</div>
                    <div class="w-24 h-1.5 bg-gray-200 dark:bg-gray-800 rounded-full overflow-hidden">
                      <div class="h-full bg-purple-500 rounded-full" :style="{ width: (split.num_rows / metadata.statistics.total_rows * 100) + '%' }" />
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Lineage & Compliance -->
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6" v-if="(metadata.lineage && metadata.lineage.length > 0) || metadata.compliance_status">
           <div class="card p-6 overflow-hidden" v-if="metadata.lineage && metadata.lineage.length > 0">
              <h4 class="font-bold text-sm uppercase text-gray-500 mb-4 flex items-center gap-2">
                <div class="i-carbon-flow-data text-blue-500" /> Data Lineage
              </h4>
              <div class="relative pl-6 border-l-2 border-dashed border-gray-200 dark:border-gray-800 space-y-6">
                <div v-for="(item, idx) in metadata.lineage" :key="idx" class="relative">
                  <div class="absolute -left-[31px] top-1 w-4 h-4 rounded-full bg-white dark:bg-gray-900 border-2 border-blue-500 shadow-sm" />
                  <div class="bg-gray-50 dark:bg-gray-900/50 p-3 rounded-xl border border-gray-100 dark:border-gray-800">
                    <div class="flex justify-between items-start mb-1">
                       <span class="text-xs font-bold text-gray-800 dark:text-gray-200">Revision: {{ item.revision.substring(0, 8) }}</span>
                       <span class="text-[10px] text-gray-400">{{ formatDate(item.created_at) }}</span>
                    </div>
                    <div class="text-[10px] text-gray-500 font-mono truncate">Script: {{ item.script_path }}</div>
                    <div v-if="item.upstream_repos && item.upstream_repos.length" class="mt-2 pt-2 border-t border-gray-100 dark:border-gray-800">
                       <div class="text-[9px] font-bold text-gray-400 uppercase mb-1">Upstream Sources</div>
                       <div class="flex flex-wrap gap-1">
                          <el-tag v-for="up in item.upstream_repos" :key="up" size="small" type="info" plain class="text-[9px]">{{ up }}</el-tag>
                       </div>
                    </div>
                  </div>
                </div>
              </div>
           </div>

           <div class="card p-6">
              <h4 class="font-bold text-sm uppercase text-gray-500 mb-4 flex items-center gap-2">
                <div class="i-carbon-security-shield text-emerald-500" /> Compliance Report
              </h4>
              <div class="space-y-4">
                 <div class="flex items-center gap-4 p-4 bg-gray-50 dark:bg-gray-900 border border-gray-100 dark:border-gray-800 rounded-2xl">
                    <div class="p-3 bg-emerald-500 text-white rounded-xl shadow-lg shadow-emerald-500/20">
                       <div class="i-carbon-checkmark-filled text-2xl" v-if="metadata.compliance_status !== 'flagged'" />
                       <div class="i-carbon-warning-alt-filled text-2xl" v-else />
                    </div>
                    <div>
                       <div class="text-sm font-bold text-gray-900 dark:text-white">
                          Status: {{ metadata.compliance_status === 'flagged' ? 'Issues Found' : 'Verified Clean' }}
                       </div>
                       <div class="text-[10px] text-gray-500">Automated sensitive data scan</div>
                    </div>
                 </div>
                 <div v-if="metadata.compliance_report" class="p-3 bg-red-50 dark:bg-red-900/10 border border-red-100 dark:border-red-900/30 rounded-xl text-xs text-red-600 dark:text-red-400">
                    <div class="font-bold mb-1 uppercase tracking-tight">Identified Risks:</div>
                    <ul class="list-disc list-inside space-y-0.5">
                       <li v-for="(val, key) in metadata.compliance_report.matches" :key="key">{{ val }}</li>
                    </ul>
                 </div>
                 <div class="text-[10px] text-gray-400 italic">
                    Note: Our automated scanner checks for PII (emails, phones, etc.) and common sensitive keywords.
                 </div>
              </div>
           </div>
        </div>

        <!-- Details & Metadata Extra Info -->
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6" v-if="metadata.description || metadata.homepage || metadata.license">
           <div class="card p-6 border-l-4 border-l-blue-500">
              <h4 class="font-bold text-sm uppercase text-gray-500 mb-3 flex items-center gap-2">
                <div class="i-carbon-information" /> Description
              </h4>
              <p class="text-sm text-gray-700 dark:text-gray-300 leading-relaxed italic">{{ metadata.description || 'No description provided.' }}</p>
           </div>
           
           <div class="card p-6 border-l-4 border-l-amber-500 flex flex-col justify-between">
              <div>
                <h4 class="font-bold text-sm uppercase text-gray-500 mb-4 flex items-center gap-2">
                  <div class="i-carbon-license" /> Metadata Summary
                </h4>
                <div class="space-y-4">
                  <div v-if="metadata.license" class="flex justify-between items-center bg-gray-50 dark:bg-gray-900 p-3 rounded-xl border border-gray-100 dark:border-gray-800">
                    <span class="text-xs font-bold text-gray-400">LICENSE</span>
                    <el-tag effect="dark" type="success" size="small">{{ metadata.license }}</el-tag>
                  </div>
                  <div v-if="metadata.homepage" class="flex justify-between items-center bg-gray-50 dark:bg-gray-900 p-3 rounded-xl border border-gray-100 dark:border-gray-800 overflow-hidden">
                    <span class="text-xs font-bold text-gray-400 shrink-0">URL</span>
                    <a :href="metadata.homepage" target="_blank" class="text-xs text-blue-500 hover:underline truncate ml-4">{{ metadata.homepage }}</a>
                  </div>
                </div>
              </div>
           </div>
        </div>
      </div>
    </transition>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import axios from 'axios'
import { ElMessage, ElMessageBox } from 'element-plus'

const props = defineProps({
  namespace: String,
  name: String,
  config: String
})

const loading = ref(true)
const error = ref(null)
const metadata = ref(null)

onMounted(async () => {
  await loadMetadata()
})

watch(() => props.config, async () => {
  await loadMetadata()
})

async function loadMetadata() {
  loading.value = true
  error.value = null
  
  try {
    const params = {}
    if (props.config) {
      params.config = props.config
    }
    
    const response = await axios.get(
      `/api/datasets/${props.namespace}/${props.name}/metadata`,
      { params }
    )
    
    metadata.value = response.data
    
    if (metadata.value.error) {
      error.value = metadata.value.error
    }
  } catch (err) {
    error.value = err.response?.data?.detail || err.message
    ElMessage.error('Failed to load dataset metadata')
  } finally {
    loading.value = false
  }
}

function formatNumber(num) {
  if (!num) return '0'
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M'
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K'
  return num.toLocaleString()
}

function formatSize(bytes) {
  if (!bytes) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i]
}

function getTypeColor(type) {
  const colors = {
    'Value': 'bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-200/50 dark:border-blue-900/50',
    'ClassLabel': 'bg-purple-500/10 text-purple-600 dark:text-purple-400 border border-purple-200/50 dark:border-purple-900/50',
    'Sequence': 'bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border border-emerald-200/50 dark:border-emerald-900/50',
    'Image': 'bg-pink-500/10 text-pink-600 dark:text-pink-400 border border-pink-200/50 dark:border-pink-900/50',
    'Audio': 'bg-orange-500/10 text-orange-600 dark:text-orange-400 border border-orange-200/50 dark:border-orange-900/50',
    'default': 'bg-gray-500/10 text-gray-600 dark:text-gray-400 border border-gray-200/50 dark:border-gray-900/50'
  }
  return colors[type] || colors.default
}

function getTypeIcon(type) {
  const icons = {
    'Value': 'i-carbon-number-1',
    'ClassLabel': 'i-carbon-tag',
    'Sequence': 'i-carbon-list',
    'Image': 'i-carbon-image',
    'Audio': 'i-carbon-microphone',
    'numeric': 'i-carbon-chart-line',
    'text': 'i-carbon-text-annotation',
    'categorical': 'i-carbon-tag',
    'image': 'i-carbon-image',
    'audio': 'i-carbon-microphone',
    'boolean': 'i-carbon-checkmark-outline',
    'sequence': 'i-carbon-list',
    'other': 'i-carbon-unknown'
  }
  return icons[type] || icons.other
}

function getTypeIconColor(type) {
  const colors = {
    'numeric': '#3b82f6',
    'text': '#10b981',
    'categorical': '#8b5cf6',
    'image': '#ec4899',
    'audio': '#f59e0b',
    'boolean': '#06b6d4',
    'sequence': '#14b8a6',
  }
  return colors[type] || '#6b7280'
}

function getFeatureDetails(feature) {
  if (feature.dtype) {
    return feature.dtype
  }
  if (feature.num_classes) {
    return `${feature.num_classes} classes`
  }
  if (feature.feature) {
    return `Sequence of ${feature.feature.type || 'items'}`
  }
  return '-'
}

function formatDate(dateStr) {
  if (!dateStr) return '-'
  return new Date(dateStr).toLocaleDateString()
}

// Logic for quality metrics (placeholder for real data from split stats)
const splitStats = ref(null)

async function showFeatureStats(name) {
   // Fetch split statistics if not already loaded
   if (!splitStats.value) {
     try {
       const response = await axios.get(`/api/datasets/${props.namespace}/${props.name}/splits/train/statistics`, {
         params: { config: props.config }
       })
       splitStats.value = response.data.column_statistics
     } catch (err) {
       ElMessage.error('Failed to load column statistics')
       return
     }
   }
   
   const stats = splitStats.value[name]
   if (stats) {
     // Show detail dialog (simplified for now)
     ElMessageBox.alert(
       `
       <div class="space-y-4 py-4">
          <div class="grid grid-cols-2 gap-4">
             <div class="p-3 bg-gray-50 rounded-lg">
                <div class="text-[10px] text-gray-400 font-bold uppercase">Mean / Median</div>
                <div class="text-sm font-mono">${stats.mean?.toFixed(4) || '-' } / ${stats.median?.toFixed(4) || '-'}</div>
             </div>
             <div class="p-3 bg-gray-50 rounded-lg">
                <div class="text-[10px] text-gray-400 font-bold uppercase">Std Dev / Skew</div>
                <div class="text-sm font-mono">${stats.std_dev?.toFixed(4) || '-' } / ${stats.skew?.toFixed(4) || '-'}</div>
             </div>
          </div>
          <div class="space-y-2">
             <div class="flex justify-between text-xs">
                <span>Null/NaN Count</span>
                <span class="font-mono text-red-500">${stats.null_count + (stats.nan_count || 0)}</span>
             </div>
             <div class="flex justify-between text-xs">
                <span>Unique Count</span>
                <span class="font-mono text-blue-500">${stats.unique_count || '-'}</span>
             </div>
             <div class="flex justify-between text-xs">
                <span>Outliers</span>
                <span class="font-mono text-amber-500">${stats.outlier_count || 0}</span>
             </div>
          </div>
          ${stats.avg_text_length ? `<div class="p-3 bg-gray-50 rounded-lg"><div class="text-[10px] text-gray-400 font-bold uppercase">Avg Text Length</div><div class="text-sm font-mono">${stats.avg_text_length.toFixed(1)} chars</div></div>` : ''}
          ${stats.image_stats ? `
             <div class="p-3 bg-gray-50 rounded-lg">
                <div class="text-[10px] text-gray-400 font-bold uppercase">Resolution (Avg / Min / Max)</div>
                <div class="text-xs font-mono">${stats.image_stats.avg_width.toFixed(0)}x${stats.image_stats.avg_height.toFixed(0)} | ${stats.image_stats.min_size[0]}x${stats.image_stats.min_size[1]} | ${stats.image_stats.max_size[0]}x${stats.image_stats.max_size[1]}</div>
             </div>
          ` : ''}
          ${stats.label_distribution ? `
             <div class="space-y-1">
                <div class="text-[10px] text-gray-400 font-bold uppercase mb-2">Class Distribution</div>
                <div class="space-y-2 max-h-40 overflow-y-auto pr-2 custom-scrollbar text-[10px]">
                   ${Object.entries(stats.label_distribution).map(([label, count]) => `
                      <div class="flex flex-col gap-1 mb-2">
                         <div class="flex justify-between font-bold">
                            <span class="truncate max-w-[150px]">${label}</span>
                            <span>${count} (${((count/stats.count)*100).toFixed(1)}%)</span>
                         </div>
                         <div class="w-full h-1 bg-gray-100 rounded-full overflow-hidden">
                            <div class="h-full bg-blue-500" style="width: ${(count/stats.count)*100}%"></div>
                         </div>
                      </div>
                   `).join('')}
                </div>
             </div>
          ` : ''}
       </div>
       `,
       `Column Statistics: ${name}`,
       {
         dangerouslyUseHTMLString: true,
         confirmButtonText: 'Close',
         roundButton: true,
         customClass: 'stats-dialog'
       }
     )
   }
}

function getQualityLabel(name) {
  if (!splitStats.value) return null
  const stats = splitStats.value[name]
  if (!stats) return null
  
  if (stats.most_common?.includes('FLAGGED')) return 'PII Risk'
  if (stats.null_count > 0 || (stats.nan_count || 0) > 0) return 'Dirty Data'
  if (Math.abs(stats.skew) > 2) return 'Skewed'
  if (stats.outlier_count > 0) return 'Outliers'
  return null
}

function getQualityType(name) {
  const label = getQualityLabel(name)
  if (label === 'PII Risk') return 'danger'
  if (label === 'Dirty Data') return 'warning'
  if (label === 'Skewed') return 'info'
  if (label === 'Outliers') return 'warning'
  return 'info'
}

function getQualityIcon(name) {
  const label = getQualityLabel(name)
  if (label === 'PII Risk') return 'i-carbon-security-shield'
  if (label === 'Dirty Data') return 'i-carbon-trash-can'
  if (label === 'Skewed') return 'i-carbon-chart-error-bar'
  if (label === 'Outliers') return 'i-carbon-chart-scatter'
  return 'i-carbon-face-satisfied'
}

function getQualityContent(name) {
  const stats = splitStats.value[name]
  if (!stats) return ''
  
  const issues = []
  if (stats.most_common?.includes('FLAGGED')) issues.push('Possible PII or sensitive content detected')
  if (stats.null_count > 0) issues.push(`${stats.null_count} null values`)
  if (stats.nan_count > 0) issues.push(`${stats.nan_count} NaN values`)
  if (Math.abs(stats.skew) > 2) issues.push('High value distribution skew')
  if (stats.outlier_count > 0) issues.push(`${stats.outlier_count} detected outliers (Z > 3)`)
  
  return issues.join(' | ')
}

onMounted(async () => {
    // Also try to pre-fetch stats for 'train' split
    try {
       const response = await axios.get(`/api/datasets/${props.namespace}/${props.name}/splits/train/statistics`, {
         params: { config: props.config }
       })
       splitStats.value = response.data.column_statistics
     } catch (err) {
       // Silent fail
     }
})
async function freezeDataset() {
   try {
      await ElMessageBox.confirm(
         'This will freeze the current dataset revision and generate a signed snapshot for production use. This action ensures immutability for audit purposes.',
         'Freeze Dataset Version',
         {
            confirmButtonText: 'Freeze',
            cancelButtonText: 'Cancel',
            type: 'warning',
            roundButton: true
         }
      )
      
      const response = await axios.post(`/api/datasets/${props.namespace}/${props.name}/snapshot`, {}, {
         params: { revision: 'main' }
      })
      
      ElMessage.success('Dataset version frozen successfully')
      await loadMetadata()
   } catch (err) {
      if (err !== 'cancel') {
         ElMessage.error(err.response?.data?.detail || 'Failed to freeze dataset')
      }
   }
}
</script>

<style scoped>
.dataset-metadata-panel {
    perspective: 1000px;
}

.stat-box {
    transform-origin: center;
}

.fade-enter-active, .fade-leave-active {
  transition: opacity 0.3s ease, transform 0.3s ease;
}
.fade-enter-from, .fade-leave-to {
  opacity: 0;
  transform: translateY(10px);
}

.custom-scrollbar::-webkit-scrollbar {
  width: 4px;
}
.custom-scrollbar::-webkit-scrollbar-thumb {
  background: #cbd5e1;
  border-radius: 10px;
}
.dark .custom-scrollbar::-webkit-scrollbar-thumb {
  background: #334155;
}
</style>

