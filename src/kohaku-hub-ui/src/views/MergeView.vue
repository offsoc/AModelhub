<template>
  <div class="merge-view p-6 max-w-4xl mx-auto">
    <h1 class="text-2xl font-bold mb-4">Merge Branches</h1>
    
    <div class="flex items-center gap-4 mb-8 p-4 bg-gray-50 rounded-lg">
      <div class="flex-1">
        <label class="text-sm text-gray-500">Source</label>
        <div class="font-mono">{{ sourceBranch }}</div>
      </div>
      <div class="text-gray-400">→</div>
      <div class="flex-1">
        <label class="text-sm text-gray-500">Target</label>
        <div class="font-mono">{{ targetBranch }}</div>
      </div>
    </div>

    <div v-if="loading" class="text-center py-12">
      <div class="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-600 mx-auto"></div>
      <p class="mt-4 text-gray-500">Executing merge...</p>
    </div>

    <div v-else-if="result" class="result-container">
      <div v-if="result.status === 'success'" class="bg-green-50 border border-green-200 p-4 rounded-lg">
        <h2 class="text-green-800 font-bold">✓ Merge Successful</h2>
        <p class="text-green-700 mt-2">{{ result.message }}</p>
        <router-link :to="`/repo/${repoId}`" class="mt-4 inline-block text-green-800 underline">Back to Repo</router-link>
      </div>

      <div v-else-if="result.status === 'conflict'" class="bg-amber-50 border border-amber-200 p-4 rounded-lg">
        <h2 class="text-amber-800 font-bold">⚠ Conflicts Detected</h2>
        <p class="text-amber-700 mt-2">The following files have conflicts and require manual resolution:</p>
        <ul class="list-disc ml-6 mt-2 text-amber-700">
          <li v-for="file in result.conflicts" :key="file">{{ file }}</li>
        </ul>
        
        <div class="mt-6 bg-gray-900 text-gray-100 p-4 rounded overflow-x-auto font-mono text-sm">
          <pre>{{ result.diff }}</pre>
        </div>
        
        <div class="mt-6 flex gap-4">
          <button @click="resolveManual" class="bg-amber-600 text-white px-4 py-2 rounded hover:bg-amber-700">Resolve in Web IDE</button>
          <button @click="cancelMerge" class="border border-gray-300 px-4 py-2 rounded hover:bg-gray-100">Cancel</button>
        </div>
      </div>
    </div>

    <div v-else class="actions">
      <p class="mb-6 text-gray-600">You are about to merge changes from <b>{{ sourceBranch }}</b> into <b>{{ targetBranch }}</b>. This operation will create a new merge commit.</p>
      <button @click="performMerge" class="bg-indigo-600 text-white px-6 py-3 rounded-lg font-bold hover:bg-indigo-700 transition-colors w-full">
        Confirm Merge
      </button>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import axios from 'axios'

const route = useRoute()
const router = useRouter()

const repoId = route.params.repoId
const sourceBranch = route.query.source || 'dev'
const targetBranch = route.query.target || 'main'

const loading = ref(false)
const result = ref(null)

const performMerge = async () => {
  loading.value = true
  try {
    const { data } = await axios.post(`/api/version/${repoId}/merge/${sourceBranch}/${targetBranch}`)
    result.value = data
  } catch (err) {
    alert('Merge failed: ' + (err.response?.data?.detail || err.message))
  } finally {
    loading.value = false
  }
}

const resolveManual = () => {
  router.push(`/repo/${repoId}/ide?branch=${targetBranch}`)
}

const cancelMerge = () => {
  router.push(`/repo/${repoId}`)
}
</script>

<style scoped>
pre {
  white-space: pre-wrap;
  word-wrap: break-word;
}
</style>
