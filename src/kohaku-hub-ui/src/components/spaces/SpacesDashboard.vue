<script setup>
import { ref, onMounted } from 'vue';

const spaces = ref([]);
const isLoading = ref(false);

const fetchSpaces = async () => {
  isLoading.value = true;
  try {
    const res = await fetch('/api/spaces/list');
    spaces.value = await res.json();
  } catch (e) {
    console.error("Failed to fetch space list", e);
  } finally {
    isLoading.value = false;
  }
};

const stopSpace = async (repo_id) => {
  try {
    await fetch(`/api/spaces/stop/${repo_id}`, { method: 'POST' });
    await fetchSpaces();
  } catch (e) {
    alert("Stop failed");
  }
};

onMounted(fetchSpaces);
</script>

<template>
  <div class="spaces-dashboard p-8">
    <div class="header mb-8 flex justify-between items-center">
      <div>
        <h1 class="text-3xl font-bold text-gray-900 dark:text-white">Active Spaces</h1>
        <p class="text-gray-500 mt-2">Monitoring and managing all running Gradio deployments.</p>
      </div>
      <button @click="fetchSpaces" class="p-2 hover:bg-gray-100 rounded-full transition-colors">
        <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>
      </button>
    </div>

    <div v-if="spaces.length === 0" class="text-center py-24 bg-white dark:bg-gray-800 rounded-2xl border border-gray-100 dark:border-gray-700 shadow-sm">
       <p class="text-gray-400">No active spaces found.</p>
    </div>

    <div v-else class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      <div v-for="space in spaces" :key="space.repo_id" class="space-card bg-white dark:bg-gray-800 rounded-xl border border-gray-100 dark:border-gray-700 shadow-sm overflow-hidden hover:shadow-md transition-shadow">
        <div class="p-5">
          <div class="flex justify-between items-start mb-4">
            <span class="px-2 py-1 bg-indigo-50 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 text-[10px] font-bold rounded uppercase tracking-wider">Gradio</span>
            <span :class="['px-2 py-1 text-[10px] font-bold rounded uppercase tracking-wider', space.status === 'running' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700']">
              {{ space.status }}
            </span>
          </div>
          <h3 class="font-bold text-gray-900 dark:text-white truncate mb-1" :title="space.repo_id">{{ space.repo_id }}</h3>
          <p class="text-xs text-gray-500 mb-4">Port: {{ space.port || 'N/A' }}</p>
          
          <div class="flex space-x-2">
            <a v-if="space.status === 'running'" :href="`/api/spaces/status/${space.repo_id}`" target="_blank" class="flex-1 py-2 bg-gray-100 hover:bg-gray-200 text-gray-800 text-xs font-bold rounded text-center transition-colors">Open View</a>
            <button @click="stopSpace(space.repo_id)" class="px-4 py-2 bg-red-50 hover:bg-red-100 text-red-600 text-xs font-bold rounded transition-colors">Stop</button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.spaces-dashboard { min-height: 80vh; }
</style>
