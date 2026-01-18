<script setup>
import { ref, computed } from 'vue'
import { ElMessage } from 'element-plus'

const props = defineProps({
  visible: Boolean,
  namespace: String,
  name: String
})

const emit = defineEmits(['update:visible'])

const show = computed({
  get: () => props.visible,
  set: (val) => emit('update:visible', val)
})

const activeTab = ref('datasets')
const copied = ref(false)

const datasetId = computed(() => `${props.namespace}/${props.name}`)

const snippets = computed(() => ({
  datasets: `from datasets import load_dataset

# Load the full dataset
dataset = load_dataset("${datasetId.value}")

# Load a specific split
train_dataset = load_dataset("${datasetId.value}", split="train")

# Stream large datasets
dataset = load_dataset("${datasetId.value}", streaming=True)

# Access data
for example in dataset["train"]:
    print(example)`,
  
  pandas: `from datasets import load_dataset
import pandas as pd

# Load dataset and convert to pandas
dataset = load_dataset("${datasetId.value}")
df = dataset["train"].to_pandas()

# Or load directly
df = pd.read_parquet("hf://datasets/${datasetId.value}/train.parquet")

print(df.head())`,
  
  polars: `from datasets import load_dataset
import polars as pl

# Load dataset and convert to polars
dataset = load_dataset("${datasetId.value}")
df = dataset["train"].to_polars()

# Or use polars scan for lazy evaluation
df = pl.scan_parquet("hf://datasets/${datasetId.value}/**/*.parquet")

print(df.head())`,
  
  duckdb: `import duckdb

# Query dataset directly with DuckDB
con = duckdb.connect()
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

# Query the dataset
con.execute("""
    SELECT * FROM 'hf://datasets/${datasetId.value}/train.parquet'
    LIMIT 10
""")
print(con.fetchdf())`,
  
  pytorch: `from datasets import load_dataset
from torch.utils.data import DataLoader

# Load dataset
dataset = load_dataset("${datasetId.value}")

# Set format for PyTorch
dataset.set_format(type="torch", columns=["input_ids", "label"])

# Create DataLoader
train_loader = DataLoader(
    dataset["train"], 
    batch_size=32, 
    shuffle=True
)

for batch in train_loader:
    print(batch)
    break`,
  
  tensorflow: `from datasets import load_dataset
import tensorflow as tf

# Load dataset
dataset = load_dataset("${datasetId.value}")

# Convert to TensorFlow dataset
tf_dataset = dataset["train"].to_tf_dataset(
    columns=["input_ids"],
    label_cols=["label"],
    batch_size=32,
    shuffle=True
)

for batch in tf_dataset:
    print(batch)
    break`
}))

async function copySnippet(tab = activeTab.value) {
  try {
    await navigator.clipboard.writeText(snippets.value[tab])
    copied.value = true
    setTimeout(() => {
      copied.value = false
    }, 2000)
    ElMessage.success('Copied to clipboard')
  } catch (err) {
    ElMessage.error('Failed to copy')
  }
}

const tabs = [
  { key: 'datasets', label: 'Datasets', icon: 'i-carbon-data-set' },
  { key: 'pandas', label: 'Pandas', icon: 'i-carbon-pandas' },
  { key: 'polars', label: 'Polars', icon: 'i-carbon-chart-polar' },
  { key: 'duckdb', label: 'DuckDB', icon: 'i-carbon-data-base' },
  { key: 'pytorch', label: 'PyTorch', icon: 'i-carbon-model' },
  { key: 'tensorflow', label: 'TensorFlow', icon: 'i-carbon-machine-learning-model' }
]
</script>

<template>
  <el-dialog
    v-model="show"
    title="Use this Dataset"
    width="800px"
    class="use-in-datasets-modal"
  >
    <div class="modal-content">
      <p class="text-sm text-gray-600 dark:text-gray-400 mb-4">
        Choose your preferred library or framework to get started with this dataset.
      </p>

      <!-- Tab Navigation -->
      <div class="flex gap-1 border-b border-gray-200 dark:border-gray-700 mb-4 overflow-x-auto">
        <button
          v-for="tab in tabs"
          :key="tab.key"
          @click="activeTab = tab.key"
          :class="[
            'px-3 py-2 text-sm font-medium transition-colors whitespace-nowrap flex items-center gap-1',
            activeTab === tab.key
              ? 'border-b-2 border-blue-500 text-blue-600 dark:text-blue-400'
              : 'text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200'
          ]"
        >
          <div :class="tab.icon" class="text-base" />
          {{ tab.label }}
        </button>
      </div>

      <!-- Code Snippet -->
      <div class="snippet-box bg-gray-50 dark:bg-gray-900 rounded-md border border-gray-200 dark:border-gray-700 p-4 relative group">
        <pre class="text-sm font-mono text-gray-800 dark:text-gray-200 overflow-x-auto whitespace-pre-wrap max-h-96 overflow-y-auto"><code>{{ snippets[activeTab] }}</code></pre>
        
        <div class="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity">
          <button 
            class="px-3 py-1 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded shadow-sm text-xs font-medium hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 cursor-pointer"
            @click="copySnippet()"
          >
            <div class="flex items-center gap-1">
               <div :class="copied ? 'i-carbon-checkmark text-green-500' : 'i-carbon-copy'" />
               {{ copied ? 'Copied!' : 'Copy' }}
            </div>
          </button>
        </div>
      </div>

      <!-- Installation Instructions -->
      <div class="mt-4 p-3 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded">
        <div class="flex items-start gap-2">
          <div class="i-carbon-information text-blue-600 dark:text-blue-400 flex-shrink-0 mt-0.5" />
          <div class="text-xs text-blue-900 dark:text-blue-100">
            <p class="font-semibold mb-1">Installation required:</p>
            <code class="block bg-white dark:bg-gray-800 p-2 rounded font-mono mt-1" v-if="activeTab === 'datasets'">
              pip install datasets
            </code>
            <code class="block bg-white dark:bg-gray-800 p-2 rounded font-mono mt-1" v-else-if="activeTab === 'pandas'">
              pip install datasets pandas pyarrow
            </code>
            <code class="block bg-white dark:bg-gray-800 p-2 rounded font-mono mt-1" v-else-if="activeTab === 'polars'">
              pip install datasets polars
            </code>
            <code class="block bg-white dark:bg-gray-800 p-2 rounded font-mono mt-1" v-else-if="activeTab === 'duckdb'">
              pip install duckdb
            </code>
            <code class="block bg-white dark:bg-gray-800 p-2 rounded font-mono mt-1" v-else-if="activeTab === 'pytorch'">
              pip install datasets torch
            </code>
            <code class="block bg-white dark:bg-gray-800 p-2 rounded font-mono mt-1" v-else-if="activeTab === 'tensorflow'">
              pip install datasets tensorflow
            </code>
          </div>
        </div>
      </div>

      <!-- Additional Tips -->
      <div class="mt-4 text-xs text-gray-600 dark:text-gray-400 space-y-1">
        <p class="flex items-center gap-1">
          <div class="i-carbon-idea text-yellow-600 dark:text-yellow-400" />
          <span><strong>Tip:</strong> For large datasets, use streaming mode to avoid downloading everything at once.</span>
        </p>
        <p class="flex items-center gap-1">
          <div class="i-carbon-document text-blue-600 dark:text-blue-400" />
          <span>
            Learn more in the 
            <a href="https://huggingface.co/docs/datasets" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">
              Datasets documentation
            </a>
          </span>
        </p>
      </div>
    </div>
  </el-dialog>
</template>

<style scoped>
.snippet-box pre {
  scrollbar-width: thin;
  scrollbar-color: rgb(209 213 219) transparent;
}

.dark .snippet-box pre {
  scrollbar-color: rgb(75 85 99) transparent;
}
</style>
