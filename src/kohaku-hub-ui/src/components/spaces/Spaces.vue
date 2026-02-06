<script setup>
import { ref, onMounted, onUnmounted } from 'vue';

const props = defineProps({
  repoId: {
    type: String,
    required: true
  },
  revision: {
    type: String,
    default: 'main'
  }
});

const status = ref({ status: 'stopped' });
const isLoading = ref(false);
const pollTimer = ref(null);

const fetchStatus = async () => {
  try {
    const res = await fetch(`/api/spaces/status/${props.repoId}`);
    status.value = await res.json();
  } catch (e) {
    console.error("Failed to fetch space status", e);
  }
};

const deploy = async () => {
  isLoading.value = true;
  try {
    const res = await fetch(`/api/spaces/deploy/${props.repoId}/${props.revision}`, { method: 'POST' });
    const data = await res.json();
    await fetchStatus();
  } catch (e) {
    alert("Deployment failed: " + e.message);
  } finally {
    isLoading.value = false;
  }
};

const stop = async () => {
  isLoading.value = true;
  try {
    await fetchStatus();
    if (status.value.status !== 'stopped') {
       await fetch(`/api/spaces/stop/${props.repoId}`, { method: 'POST' });
       await fetchStatus();
    }
  } catch (e) {
    alert("Stop failed");
  } finally {
    isLoading.value = false;
  }
};

onMounted(() => {
  fetchStatus();
  pollTimer.value = setInterval(fetchStatus, 5000);
});

onUnmounted(() => {
  if (pollTimer.value) clearInterval(pollTimer.value);
});
</script>

<template>
  <div class="spaces-view p-6 max-w-6xl mx-auto">
    <!-- Header -->
    <div class="flex items-center justify-between mb-8 bg-white dark:bg-gray-800 p-6 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700">
      <div class="flex items-center space-x-4">
        <div class="p-3 bg-indigo-100 dark:bg-indigo-900 rounded-lg text-indigo-600">
          <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.691.34a6 6 0 01-3.86.517l-2.388-.477a2 2 0 00-1.022.547l-1.16 1.16a2 2 0 000 2.828l1.247 1.247a2 2 0 002.828 0l1.247-1.247a2 2 0 000-2.828l-1.16-1.16z"></path></svg>
        </div>
        <div>
          <h2 class="text-2xl font-bold text-gray-900 dark:text-white">Spaces</h2>
          <p class="text-gray-500 dark:text-gray-400 text-sm">Deploy Gradio apps directly from this repository.</p>
        </div>
      </div>
      
      <div class="flex items-center space-x-3">
        <div v-if="status.status !== 'stopped'" class="flex items-center space-x-2 mr-4">
           <span :class="['w-2.5 h-2.5 rounded-full animate-pulse', status.status === 'running' ? 'bg-green-500' : 'bg-yellow-500']"></span>
           <span class="text-sm font-medium uppercase tracking-wider">{{ status.status }}</span>
        </div>
        
        <button 
          v-if="status.status === 'stopped' || status.status === 'crashed'"
          @click="deploy" 
          :disabled="isLoading"
          class="px-5 py-2.5 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg font-semibold shadow-md transition-all flex items-center space-x-2"
        >
          <svg v-if="isLoading" class="animate-spin h-4 w-4 text-white" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg>
          <span>Deploy Space</span>
        </button>
        
        <button 
          v-else
          @click="stop" 
          :disabled="isLoading"
          class="px-5 py-2.5 bg-red-100 text-red-700 hover:bg-red-200 rounded-lg font-semibold transition-all flex items-center space-x-2"
        >
          <span>Stop Space</span>
        </button>
      </div>
    </div>

    <!-- Viewer -->
    <div v-if="status.status === 'running'" class="space-viewer rounded-2xl overflow-hidden shadow-2xl bg-white border border-gray-200">
      <div class="bg-gray-50 px-4 py-2 border-b flex items-center justify-between text-xs text-gray-500">
        <div class="flex items-center space-x-2">
          <span>{{ status.url }}</span>
        </div>
        <span>Running on Subprocess (Sandbox v1)</span>
      </div>
      <iframe :src="status.url" class="w-full h-[800px] border-none"></iframe>
    </div>

    <!-- Empty State -->
    <div v-else-if="status.status === 'stopped'" class="text-center py-20 bg-gray-50 dark:bg-gray-800/50 rounded-2xl border-2 border-dashed border-gray-200 dark:border-gray-700">
      <div class="mb-4 text-gray-400">
        <svg class="mx-auto w-16 h-16" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
      </div>
      <h3 class="text-lg font-bold mb-2">Ready to Launch</h3>
      <p class="text-gray-500 max-w-sm mx-auto">Upload an <code>app.py</code> and <code>requirements.txt</code> to this repository to start your Space.</p>
    </div>

    <!-- Error State -->
    <div v-else-if="status.status === 'error' || status.status === 'crashed'" class="bg-red-50 p-6 rounded-xl border border-red-100">
      <div class="flex items-start space-x-3">
        <svg class="w-6 h-6 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
        <div>
          <h4 class="font-bold text-red-800">Deployment Error</h4>
          <p class="text-red-700 text-sm mt-1">{{ status.error || 'The app crashed unexpectedly.' }}</p>
          <button @click="deploy" class="mt-4 text-sm font-bold text-red-800 underline">Try Redeploying</button>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.spaces-view { min-height: 100vh; }
</style>
