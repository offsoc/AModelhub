<script setup>
import { ref, watch, onMounted } from "vue";
import { ElMessage } from "element-plus";
import axios from "axios";
import DataGridEnhanced from "./DataGridEnhanced.vue";

const props = defineProps({
  namespace: {
    type: String,
    required: true,
  },
  name: {
    type: String,
    required: true,
  },
  branch: {
    type: String,
    default: "main",
  },
});

const loadingInfo = ref(false);
const loadingRows = ref(false);
const error = ref(null);

const configs = ref([]);
const splits = ref({}); // config -> split names
const currentConfig = ref("");
const currentSplit = ref("");
const datasetInfo = ref({});

const rows = ref([]);
const columns = ref([]);
const totalRows = ref(0); // Approximate or from split info
const offset = ref(0);
const limit = ref(100);
const whereQuery = ref("");
const showSchema = ref(false);
const showStats = ref(false);
const columnStats = ref({});
const loadingStats = ref(false);

// Fetch dataset info (configs/splits)
async function fetchInfo() {
  loadingInfo.value = true;
  error.value = null;
  try {
    const { data } = await axios.get(
      `/api/datasets/${props.namespace}/${props.name}/viewer/info?ref=${props.branch}`
    );
    datasetInfo.value = data.info;
    configs.value = data.configs;
    
    // Auto-select first config
    if (configs.value.length > 0) {
      currentConfig.value = configs.value[0];
    }
  } catch (err) {
    error.value = err.response?.data?.detail || err.message;
  } finally {
    loadingInfo.value = false;
  }
}

// Fetch rows
async function fetchRows() {
  if (!currentConfig.value || !currentSplit.value) return;
  
  loadingRows.value = true;
  error.value = null;
  try {
    const { data } = await axios.get(
      `/api/datasets/${props.namespace}/${props.name}/viewer/rows`,
      {
        params: {
          config: currentConfig.value,
          split: currentSplit.value,
          offset: offset.value,
          limit: limit.value,
          ref: props.branch,
          where: whereQuery.value || undefined
        }
      }
    );
    
    // Extract rows and infer columns if needed
    if (data.rows && data.rows.length > 0) {
      if (data.rows[0]) {
        columns.value = Object.keys(data.rows[0]);
      }
      rows.value = data.rows.map(r => Object.values(r)); // DataGrid expects array of arrays? Check DataGridEnhanced
      // DataGridEnhanced expects rows as array of arrays?
      // Let's check DataGridEnhanced props. 
      // Existing DatasetViewer passed `result.rows` which from `previewFile` in `api.js` (datasetviewer backend) returns list of lists.
      // My backend `get_dataset_rows` returns list of dicts (from datasets library).
      // I should convert dicts to lists based on columns.
      rows.value = data.rows.map(r => columns.value.map(c => r[c]));
    } else {
        rows.value = [];
    }
    
    // Calculate basic stats if we have rows
    if (rows.value.length > 0 && columns.value.length > 0) {
      calculateColumnStats();
    }
    
  } catch (err) {
    error.value = err.response?.data?.detail || err.message;
  } finally {
    loadingRows.value = false;
  }
}

function calculateColumnStats() {
  columnStats.value = {};
  
  columns.value.forEach((colName, colIdx) => {
    const values = rows.value.map(row => row[colIdx]).filter(v => v !== null && v !== undefined);
    
    const stats = {
      count: values.length,
      nullCount: rows.value.length - values.length,
    };
    
    // Try to detect type and calculate type-specific stats
    const numericValues = values.filter(v => typeof v === 'number' || !isNaN(Number(v))).map(v => Number(v));
    
    if (numericValues.length > values.length * 0.8) {
      // Numeric column
      stats.type = 'numeric';
      stats.min = Math.min(...numericValues);
      stats.max = Math.max(...numericValues);
      stats.mean = numericValues.reduce((a, b) => a + b, 0) / numericValues.length;
      stats.median = numericValues.sort((a, b) => a - b)[Math.floor(numericValues.length / 2)];
    } else {
      // String/categorical column
      stats.type = 'categorical';
      const stringValues = values.map(v => String(v));
      const uniqueValues = [...new Set(stringValues)];
      stats.uniqueCount = uniqueValues.length;
      
      // Most common value
      const valueCounts = {};
      stringValues.forEach(v => {
        valueCounts[v] = (valueCounts[v] || 0) + 1;
      });
      const mostCommon = Object.entries(valueCounts).sort((a, b) => b[1] - a[1])[0];
      if (mostCommon) {
        stats.mostCommon = mostCommon[0];
        stats.mostCommonCount = mostCommon[1];
      }
    }
    
    columnStats.value[colName] = stats;
  });
}

function getColumnTypeIcon(type) {
  const icons = {
    'numeric': 'i-carbon-chart-line',
    'categorical': 'i-carbon-categories',
    'text': 'i-carbon-text-annotation',
  };
  return icons[type] || 'i-carbon-unknown';
}

function getColumnTypeColor(type) {
  const colors = {
    'numeric': 'text-blue-600 dark:text-blue-400',
    'categorical': 'text-purple-600 dark:text-purple-400',
    'text': 'text-green-600 dark:text-green-400',
  };
  return colors[type] || 'text-gray-600 dark:text-gray-400';
}

// Watch config change to update splits
watch(currentConfig, (newConfig) => {
  if (!newConfig) return;
  const configInfo = datasetInfo.value[newConfig];
  if (configInfo && configInfo.splits) {
    const splitNames = Object.keys(configInfo.splits);
    if (splitNames.length > 0) {
      currentSplit.value = splitNames[0];
    } else {
        currentSplit.value = "";
    }
    // Update total rows estimate if available
    // (logic to sum up or just show for current split)
  }
});

// Watch split change to fetch rows
watch([currentSplit, offset], () => {
    if (currentSplit.value) {
        fetchRows();
    }
});

onMounted(() => {
  fetchInfo();
});

function handlePageChange(newOffset) {
    // Pagination logic
}

function copySchema() {
  if (currentConfig.value && datasetInfo.value[currentConfig.value]?.features) {
    navigator.clipboard.writeText(JSON.stringify(datasetInfo.value[currentConfig.value].features, null, 2));
    ElMessage.success('Schema copied to clipboard!');
  }
}

</script>

<template>
  <div class="dataset-studio">
    <!-- Configuration Bar -->
    <div class="config-bar card mb-4 p-4 flex flex-wrap gap-6 items-center bg-gray-50/80 dark:bg-gray-800/80 backdrop-blur-sm sticky top-0 z-10 border border-gray-200 dark:border-gray-700 shadow-sm">
      <template v-if="loadingInfo">
        <el-skeleton animated class="w-full">
          <template #template>
            <div class="flex gap-4">
              <el-skeleton-item variant="rect" class="w-40 h-8" />
              <el-skeleton-item variant="rect" class="w-40 h-8" />
              <el-skeleton-item variant="rect" class="flex-1 h-8" />
            </div>
          </template>
        </el-skeleton>
      </template>
      
      <template v-else-if="error">
        <div class="text-red-500 flex items-center gap-2">
            <div class="i-carbon-warning text-lg" /> 
            <span class="font-medium">{{ error }}</span>
        </div>
        <el-button size="small" type="danger" plain @click="fetchInfo">
          <div class="i-carbon-renew mr-1" /> Retry
        </el-button>
      </template>
      
      <template v-else>
         <!-- Config Selector -->
         <div class="flex flex-col">
            <label class="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">Subset</label>
            <el-select v-model="currentConfig" placeholder="Select Config" size="default" class="w-48">
                <el-option v-for="c in configs" :key="c" :label="c" :value="c" />
            </el-select>
         </div>
         
         <!-- Split Selector -->
         <div class="flex flex-col">
            <label class="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">Split</label>
            <el-select v-model="currentSplit" placeholder="Select Split" size="default" class="w-48">
                 <template v-if="currentConfig && datasetInfo[currentConfig]?.splits">
                    <el-option v-for="(info, s) in datasetInfo[currentConfig].splits" :key="s" :label="`${s} (${info.num_examples?.toLocaleString() || '?'})`" :value="s" />
                 </template>
            </el-select>
         </div>
         
          <!-- Filter Input -->
          <div class="flex flex-col flex-1 min-w-[200px]">
            <label class="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">Filter Data</label>
            <el-input 
                v-model="whereQuery" 
                placeholder="SQL-like filter (e.g. label == 1)" 
                size="default" 
                clearable 
                @keyup.enter="fetchRows"
            >
                <template #prefix>
                    <div class="i-carbon-filter text-blue-500" />
                </template>
            </el-input>
          </div>

          <div class="flex items-end gap-2 h-full self-end">
            <el-tooltip content="View Dataset Schema (Features)" placement="top">
              <el-button plain @click="showSchema = true">
                <div class="i-carbon-list-boxes text-lg" />
              </el-button>
            </el-tooltip>
            
            <el-tooltip :content="showStats ? 'Hide column statistics' : 'Calculate column statistics'" placement="top">
              <el-button :type="showStats ? 'primary' : 'default'" @click="showStats = !showStats">
                <div class="i-carbon-chart-bar text-lg" />
              </el-button>
            </el-tooltip>

            <el-button type="primary" @click="fetchRows" :loading="loadingRows">
               <div v-if="!loadingRows" class="i-carbon-play mr-1" /> {{ loadingRows ? 'Refreshing...' : 'Apply' }}
            </el-button>
          </div>
      </template>
    </div>
    
    <!-- Data Grid & Statistics -->
    <div v-if="loadingRows && rows.length === 0" class="card overflow-hidden">
        <el-skeleton :rows="10" animated class="p-4" />
    </div>
    
    <div v-else-if="rows.length > 0" class="flex gap-4 content-wrapper">
      <!-- Main Data Grid -->
      <div :class="[showStats ? 'flex-1 overflow-x-hidden' : 'w-full', 'dataset-grid-container']">
        <div class="card overflow-hidden shadow-sm hover:shadow-md transition-shadow">
          <DataGridEnhanced 
              :columns="columns" 
              :rows="rows" 
              :truncated="false"
              :column-stats="columnStats"
          />
        </div>
        
        <!-- Pagination Controls -->
        <div class="mt-4 flex justify-between items-center p-2">
            <el-button :disabled="offset === 0" plain @click="offset = Math.max(0, offset - limit)">
              <div class="i-carbon-chevron-left mr-1" /> Previous
            </el-button>
            
            <div class="flex items-center gap-4">
              <span class="text-xs font-medium text-gray-500 bg-gray-100 dark:bg-gray-800 px-3 py-1 rounded-full border border-gray-200 dark:border-gray-700">
                Rows {{ offset.toLocaleString() }} - {{ (offset + rows.length).toLocaleString() }}
              </span>
              <div class="flex items-center gap-2">
                <span class="text-xs text-gray-400">Page Size:</span>
                <el-select v-model="limit" size="small" class="w-20" @change="offset = 0; fetchRows()">
                  <el-option :label="100" :value="100" />
                  <el-option :label="250" :value="250" />
                  <el-option :label="500" :value="500" />
                </el-select>
              </div>
            </div>

            <el-button :disabled="rows.length < limit" plain @click="offset += limit">
              Next <div class="i-carbon-chevron-right ml-1" />
            </el-button>
        </div>
      </div>
      
      <!-- Statistics Sidebar -->
      <transition name="slide">
        <div v-if="showStats" class="stats-sidebar w-80 shrink-0 sticky top-24 self-start">
          <div class="card h-[calc(100vh-140px)] overflow-hidden flex flex-col border border-gray-200 dark:border-gray-700 shadow-lg">
            <div class="p-4 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between bg-gray-100/50 dark:bg-gray-800/50">
              <h3 class="font-bold text-sm flex items-center gap-2 text-gray-700 dark:text-gray-200">
                <div class="i-carbon-chart-bar text-blue-500" />
                Column Statistics
              </h3>
              <button @click="showStats = false" class="text-gray-400 hover:text-red-500 transition-colors">
                 <div class="i-carbon-close-filled text-lg" />
              </button>
            </div>
            
            <div class="flex-1 overflow-y-auto p-4 space-y-4 custom-scrollbar">
              <div v-for="(stats, colName) in columnStats" :key="colName" class="stats-card group">
                <div class="flex items-center gap-2 mb-2 p-1">
                  <div :class="[getColumnTypeIcon(stats.type), getColumnTypeColor(stats.type)]" class="text-lg group-hover:scale-110 transition-transform" />
                  <span class="font-bold text-xs truncate text-gray-800 dark:text-gray-100" :title="colName">{{ colName }}</span>
                </div>
                
                <div class="stats-grid bg-gray-50 dark:bg-gray-900/50 rounded-lg p-3 border border-gray-100 dark:border-gray-800">
                  <div class="flex justify-between items-center py-1 text-[11px] border-b border-gray-200/50 dark:border-gray-800/50">
                    <span class="text-gray-500">Validity</span>
                    <el-tag size="small" :type="stats.count > rows.length * 0.9 ? 'success' : 'warning'" effect="plain">
                      {{ ((stats.count / rows.length) * 100).toFixed(1) }}%
                    </el-tag>
                  </div>
                  
                  <template v-if="stats.type === 'numeric'">
                    <div class="flex justify-between items-center py-1 text-[11px] border-b border-gray-200/50 dark:border-gray-800/50">
                      <span class="text-gray-500">Min / Max</span>
                      <span class="font-mono">{{ stats.min?.toLocaleString() }} / {{ stats.max?.toLocaleString() }}</span>
                    </div>
                    <div class="flex justify-between items-center py-1 text-[11px] border-b border-gray-200/50 dark:border-gray-800/50">
                      <span class="text-gray-500">Mean</span>
                      <span class="font-mono text-blue-600 dark:text-blue-400">{{ stats.mean?.toFixed(2) }}</span>
                    </div>
                    <div class="flex justify-between items-center py-1 text-[11px]">
                      <span class="text-gray-500">Median</span>
                      <span class="font-mono">{{ stats.median?.toLocaleString() }}</span>
                    </div>
                  </template>
                  
                  <template v-else-if="stats.type === 'categorical'">
                    <div class="flex justify-between items-center py-1 text-[11px] border-b border-gray-200/50 dark:border-gray-800/50">
                      <span class="text-gray-500">Unique Values</span>
                      <span class="font-mono font-bold">{{ stats.uniqueCount?.toLocaleString() }}</span>
                    </div>
                    <div v-if="stats.mostCommon" class="flex flex-col gap-1 pt-2">
                      <span class="text-[10px] text-gray-400 uppercase tracking-tighter font-bold">Most Common</span>
                      <div class="flex justify-between items-center bg-white dark:bg-gray-800 p-2 rounded border border-gray-100 dark:border-gray-700">
                        <span class="font-medium text-xs truncate max-w-[120px]" :title="String(stats.mostCommon)">{{ stats.mostCommon }}</span>
                        <span class="text-blue-500 font-bold text-[10px]">{{ ((stats.mostCommonCount / stats.count) * 100).toFixed(0) }}%</span>
                      </div>
                    </div>
                  </template>
                </div>
              </div>
            </div>
          </div>
        </div>
      </transition>
    </div>
    
    <div v-else-if="!loadingInfo && !loadingRows && currentSplit" class="text-center py-32 text-gray-500 card bg-gray-50 dark:bg-gray-800/50 border-dashed">
        <div class="i-carbon-data-table text-7xl mb-4 text-gray-300 dark:text-gray-600" />
        <h3 class="text-xl font-medium mb-1">No data to display</h3>
        <p class="text-sm">Try changing the configuration or removing filters.</p>
        <el-button class="mt-6" @click="whereQuery = ''; fetchRows()">Clear Filter</el-button>
    </div>
    
    <!-- Schema Dialog -->
    <el-dialog v-model="showSchema" title="Dataset Features (Schema)" width="800px" align-center class="schema-dialog">
        <div v-if="currentConfig && datasetInfo[currentConfig]?.features">
            <div class="flex items-center gap-2 mb-4 p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg text-blue-700 dark:text-blue-300 text-sm">
              <div class="i-carbon-information" />
              <span>This schema defines the structure and types of each column in the current dataset configuration.</span>
            </div>
            <div class="relative group">
              <pre class="bg-gray-900 text-gray-100 p-6 rounded-xl text-xs font-mono overflow-auto max-h-[60vh] custom-scrollbar shadow-inner leading-relaxed">{{ JSON.stringify(datasetInfo[currentConfig].features, null, 2) }}</pre>
              <button class="absolute top-4 right-4 p-2 bg-gray-800 hover:bg-gray-700 text-gray-400 rounded-lg opacity-0 group-hover:opacity-100 transition-opacity" @click="copySchema">
                <div class="i-carbon-copy" />
              </button>
            </div>
        </div>
        <div v-else class="text-center py-10 text-gray-500">
            <div class="i-carbon-warning-alt text-4xl mb-2" />
            <p>No feature information available for this configuration.</p>
        </div>
    </el-dialog>
  </div>
</template>

<style scoped>
.dataset-studio {
  animation: fadeIn 0.4s ease-out;
}

.config-bar {
  border-radius: 12px;
}

.content-wrapper {
  min-height: 400px;
}

.stats-sidebar {
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

.stats-card:hover .stats-grid {
  border-color: rgba(59, 130, 246, 0.5);
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
}

.custom-scrollbar::-webkit-scrollbar {
  width: 4px;
}
.custom-scrollbar::-webkit-scrollbar-track {
  background: transparent;
}
.custom-scrollbar::-webkit-scrollbar-thumb {
  background: #e2e8f0;
  border-radius: 10px;
}
.dark .custom-scrollbar::-webkit-scrollbar-thumb {
  background: #334155;
}

.slide-enter-active,
.slide-leave-active {
  transition: all 0.3s ease;
}
.slide-enter-from,
.slide-leave-to {
  transform: translateX(20px);
  opacity: 0;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(10px); }
  to { opacity: 1; transform: translateY(0); }
}

:deep(.el-skeleton__item) {
  border-radius: 8px;
}

:deep(.schema-dialog) {
  border-radius: 16px;
}
</style>
