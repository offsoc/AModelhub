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
                Feature Definitions
              </h3>
              <el-tag size="small" type="info" effect="dark" round>{{ Object.keys(metadata.features).length }} Features</el-tag>
            </div>
            
            <div class="overflow-x-auto min-h-[300px]">
              <table class="w-full text-sm">
                <thead class="bg-gray-50/50 dark:bg-gray-800/50 text-[10px] uppercase text-gray-400 tracking-wider">
                  <tr>
                    <th class="px-6 py-4 text-left font-bold">Field</th>
                    <th class="px-6 py-4 text-left font-bold">Type</th>
                    <th class="px-6 py-4 text-left font-bold">Details</th>
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
                      <span class="text-xs text-gray-500 dark:text-gray-400 font-mono italic">
                        {{ getFeatureDetails(feature) }}
                      </span>
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
import { ElMessage } from 'element-plus'

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

