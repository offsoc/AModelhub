<script setup>
import { computed } from 'vue';

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

const viewerUrl = computed(() => {
  return `/api/viewer/dataset/${props.repoId}/${props.revision}`;
});
</script>

<template>
  <div class="gradio-dataset-viewer border rounded-lg overflow-hidden bg-gray-50 dark:bg-gray-900 shadow-sm">
    <div class="px-4 py-3 border-b bg-white dark:bg-gray-800 flex items-center justify-between">
      <div class="flex items-center space-x-2">
        <span class="text-gray-500">
          <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 20 20"><path d="M7 3a1 1 0 000 2h6a1 1 0 100-2H7zM4 7a1 1 0 011-1h10a1 1 0 110 2H5a1 1 0 01-1-1zM2 11a2 2 0 012-2h12a2 2 0 012 2v4a2 2 0 01-2 2H4a2 2 0 01-2-2v-4z"></path></svg>
        </span>
        <h3 class="text-md font-medium text-gray-900 dark:text-gray-100">Dataset Viewer</h3>
      </div>
      <div class="flex space-x-2">
        <span class="px-2 py-0.5 text-xs font-mono bg-blue-100 text-blue-700 rounded-md">PREVIEW</span>
      </div>
    </div>
    
    <div class="relative w-full" style="height: 700px">
      <iframe 
        :src="viewerUrl" 
        class="w-full h-full border-none"
        allow="accelerometer; ambient-light-sensor; camera; encrypted-media; geolocation; gyroscope; hid; microphone; midi; payment; usb; vr; xr-spatial-tracking"
        title="Dataset Viewer"
      ></iframe>
    </div>
    
    <div class="px-4 py-2 border-t bg-white dark:bg-gray-800 text-xs text-gray-500 text-center">
      Powered by Gradio and Datasets library
    </div>
  </div>
</template>

<style scoped>
.gradio-dataset-viewer {
  transition: all 0.2s ease;
}
iframe {
  color-scheme: light;
}
</style>
