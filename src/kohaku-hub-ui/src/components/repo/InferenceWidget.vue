<script setup>
import { ref, computed } from 'vue';

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

const widgetUrl = computed(() => {
  return `/api/inference/widget/${props.repoId}`;
});

const isLoaded = ref(false);
const handleLoad = () => {
  isLoaded.value = true;
};
</script>

<template>
  <div class="inference-widget border rounded-xl overflow-hidden shadow-sm bg-white dark:bg-gray-900 mt-6">
    <div class="px-5 py-4 border-b flex items-center justify-between bg-gray-50 dark:bg-gray-800">
      <div class="flex items-center space-x-2">
        <svg class="w-5 h-5 text-indigo-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
        <span class="font-bold text-gray-800 dark:text-white">Inference API</span>
      </div>
      <div class="flex space-x-2">
        <span class="px-2 py-1 text-[10px] font-bold bg-green-100 text-green-700 rounded-full">LOCAL GPU</span>
      </div>
    </div>
    
    <div class="relative w-full" style="height: 500px">
      <div v-if="!isLoaded" class="absolute inset-0 flex items-center justify-center bg-gray-50 dark:bg-gray-800 z-10">
        <div class="flex flex-col items-center">
          <div class="spinner border-4 border-indigo-500 border-t-transparent rounded-full w-8 h-8 animate-spin"></div>
          <span class="mt-3 text-sm text-gray-500">Loading Inference Engine...</span>
        </div>
      </div>
      
      <iframe 
        :src="widgetUrl" 
        @load="handleLoad"
        class="w-full h-full border-none"
        title="Inference Widget"
      ></iframe>
    </div>
    
    <div class="px-5 py-4 bg-gray-50 dark:bg-gray-800 border-t text-xs text-gray-500 flex justify-between items-center">
      <span>Using <code>accelerate</code> auto-device-mapping</span>
      <button class="text-indigo-600 hover:underline font-medium">Settings</button>
    </div>
  </div>
</template>

<style scoped>
@keyframes spin {
  to { transform: rotate(360deg); }
}
.animate-spin {
  animation: spin 1s linear infinite;
}
</style>
