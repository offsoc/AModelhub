<template>
  <div class="history-view p-6 max-w-5xl mx-auto">
    <div class="flex justify-between items-center mb-6">
      <h1 class="text-2xl font-bold">Commit History</h1>
      <div v-if="repoInfo" class="text-sm text-gray-500 font-mono">{{ repoId }}</div>
    </div>

    <div v-if="loading" class="space-y-4">
      <div v-for="i in 5" :key="i" class="h-20 bg-gray-100 animate-pulse rounded-lg"></div>
    </div>

    <div v-else class="space-y-4">
      <div v-for="commit in commits" :key="commit.id" 
           class="commit-card border border-gray-200 p-4 rounded-lg hover:border-indigo-300 transition-colors bg-white shadow-sm">
        <div class="flex justify-between items-start">
          <div class="flex-1">
            <div class="flex items-center gap-2 mb-1">
              <span class="font-bold text-gray-900">{{ commit.message }}</span>
              <span class="text-xs font-mono bg-gray-100 px-2 py-0.5 rounded text-gray-600">{{ commit.id.substring(0, 8) }}</span>
            </div>
            <p class="text-sm text-gray-600">{{ commit.author_name }} committed {{ formatDate(commit.date) }}</p>
          </div>
          
          <div class="flex items-center gap-2">
            <button 
              @click="confirmRollback(commit)"
              class="text-sm border border-red-200 text-red-600 px-3 py-1.5 rounded hover:bg-red-50 transition-colors flex items-center gap-1"
              :disabled="rollingBack"
            >
              <span v-if="rollingBack === commit.id" class="animate-spin h-3 w-3 border-b-2 border-red-600 rounded-full"></span>
              Rollback to here
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- Rollback Confirmation Modal -->
    <div v-if="showConfirm" class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
      <div class="bg-white rounded-xl max-w-md w-full p-6 shadow-2xl">
        <h3 class="text-xl font-bold text-gray-900 mb-2">Confirm Rollback</h3>
        <p class="text-gray-600 mb-6">
          This will revert the changes from <b>{{ selectedCommit?.id.substring(0, 8) }}</b>. 
          A safety backup branch will be created in LakeFS. Are you sure?
        </p>
        <div class="flex justify-end gap-3">
          <button @click="showConfirm = false" class="px-4 py-2 text-gray-600 hover:bg-gray-100 rounded-lg">Cancel</button>
          <button @click="executeRollback" class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 font-bold">
            Confirm & Rollback
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import axios from 'axios'

const route = useRoute()
const repoId = route.params.repoId

const commits = ref([])
const loading = ref(true)
const rollingBack = ref(null)
const showConfirm = ref(false)
const selectedCommit = ref(null)

const fetchHistory = async () => {
  loading.value = true
  try {
    const { data } = await axios.get(`/api/repo/${repoId}/history`)
    commits.value = data
  } catch (err) {
    console.error('Failed to fetch history:', err)
  } finally {
    loading.value = false
  }
}

const confirmRollback = (commit) => {
  selectedCommit.value = commit
  showConfirm.value = true
}

const executeRollback = async () => {
  if (!selectedCommit.value) return
  
  const commitId = selectedCommit.value.id
  rollingBack.value = commitId
  showConfirm.value = false
  
  try {
    const { data } = await axios.post(`/api/version/${repoId}/rollback/${commitId}`)
    if (data.status === 'success') {
      alert(`Rollback successful! Backup created at: ${data.backup_branch}`)
      fetchHistory() // Refresh listing
    } else {
      alert('Rollback failed: ' + data.message)
    }
  } catch (err) {
    alert('Error executing rollback: ' + (err.response?.data?.detail || err.message))
  } finally {
    rollingBack.value = null
  }
}

const formatDate = (dateStr) => {
  return new Date(dateStr).toLocaleDateString(undefined, { 
    month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' 
  })
}

onMounted(fetchHistory)
</script>
