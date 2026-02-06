<script setup>
import { computed } from 'vue';

const props = defineProps({
  repoId: {
    type: String,
    required: true
  }
});

const widgetUrl = computed(() => {
  return `/api/viewer/model/${props.repoId}`;
});
</script>

<template>
  <div class="model-widget border rounded-lg overflow-hidden bg-white dark:bg-gray-900 shadow-sm sm:max-w-md ml-auto">
    <div class="px-4 py-3 border-b flex items-center justify-between bg-gray-50 dark:bg-gray-800">
      <div class="flex items-center space-x-2">
        <svg class="w-4 h-4 text-yellow-500" fill="currentColor" viewBox="0 0 20 20"><path d="M11 3a1 1 0 10-2 0v1a1 1 0 102 0V3zM15.657 5.757a1 1 0 00-1.414-1.414l-.707.707a1 1 0 001.414 1.414l.707-.707zM18 10a1 1 0 01-1 1h-1a1 1 0 110-2h1a1 1 0 011 1zM5.05 6.464A1 1 0 106.464 5.05l-.707-.707a1 1 0 00-1.414 1.414l.707.707zM5 10a1 1 0 01-1 1H3a1 1 0 110-2h1a1 1 0 011 1zM8 16v-1a1 1 0 112 0v1a1 1 0 11-2 0zM13.536 14.95a1 1 0 011.414 0l.707.707a1 1 0 11-1.414 1.414l-.707-.707a1 1 0 010-1.414zM16.121 17.243a1 1 0 01-1.414 0l-.707-.707a1 1 0 111.414-1.414l.707.707a1 1 0 010 1.414zM4.929 16.364a1 1 0 010-1.414l.707-.707a1 1 0 111.414 1.414l-.707.707a1 1 0 01-1.414 0z"></path></svg>
        <span class="text-sm font-semibold text-gray-700 dark:text-gray-200">Inference Widget</span>
      </div>
      <div class="flex space-x-1">
        <button class="p-1 hover:bg-gray-200 rounded text-gray-400"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 5v.01M12 12v.01M12 19v.01M12 6a1 1 0 110-2 1 1 0 010 2zm0 7a1 1 0 110-2 1 1 0 010 2zm0 7a1 1 0 110-2 1 1 0 010 2z"></path></svg></button>
      </div>
    </div>
    
    <div class="relative w-full overflow-hidden" style="height: 450px">
      <iframe 
        :src="widgetUrl" 
        class="w-full h-full border-none"
        title="Model Inference Widget"
      ></iframe>
    </div>
    
    <div class="px-4 py-3 bg-gray-50 dark:bg-gray-800 border-t flex flex-wrap gap-2">
      <button class="px-3 py-1.5 text-sm bg-white border border-gray-300 rounded hover:bg-gray-50 flex items-center space-x-1 shadow-sm">
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3"></path></svg>
        <span>Use in Transformers</span>
      </button>
      <button class="px-3 py-1.5 text-sm bg-black text-white rounded hover:bg-gray-900 shadow-sm">
        Train
      </button>
    </div>
  </div>
</template>

<style scoped>
.model-widget {
  max-width: 400px;
}
iframe {
  filter: contrast(0.95);
}
</style>
