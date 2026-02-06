<template>
  <div class="card-editor p-6 max-w-6xl mx-auto h-[calc(100vh-100px)] flex flex-col">
    <div class="flex justify-between items-center mb-4">
      <h1 class="text-2xl font-bold">Model Card Editor</h1>
      <div class="flex gap-2">
        <button @click="saveCard" class="bg-indigo-600 text-white px-4 py-2 rounded font-bold hover:bg-indigo-700">Save README</button>
        <button @click="back" class="border p-2 rounded hover:bg-gray-50">Cancel</button>
      </div>
    </div>

    <div class="flex-1 flex gap-4 overflow-hidden">
      <!-- Editor -->
      <div class="flex-1 flex flex-col border rounded-lg">
        <div class="bg-gray-50 border-b p-2 text-xs font-bold text-gray-500 uppercase">Markdown Editor</div>
        <textarea 
          v-model="content" 
          class="flex-1 p-4 font-mono text-sm outline-none resize-none"
          placeholder="Enter model card markdown with YAML frontmatter..."
        ></textarea>
      </div>

      <!-- Preview -->
      <div class="flex-1 flex flex-col border rounded-lg overflow-hidden">
        <div class="bg-gray-50 border-b p-2 text-xs font-bold text-gray-500 uppercase">HuggingFace Preview</div>
        <div class="flex-1 p-6 overflow-y-auto prose prose-indigo max-w-none bg-white">
          <div v-if="metadata" class="mb-6 p-4 bg-gray-50 rounded border text-sm">
             <div class="font-bold mb-2">Model Metadata</div>
             <div class="grid grid-cols-2 gap-2">
               <div>License: <span class="font-mono">{{ metadata.license }}</span></div>
               <div>Library: <span class="font-mono">{{ metadata.library_name }}</span></div>
               <div class="col-span-2">Tags: {{ metadata.tags?.join(', ') }}</div>
             </div>
          </div>
          <div v-html="previewHtml"></div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import axios from 'axios'
import { marked } from 'marked'
import yaml from 'js-yaml'

const route = useRoute()
const router = useRouter()
const repoId = route.params.repoId

const content = ref('')
const loading = ref(true)

const fetchCard = async () => {
  try {
    const { data } = await axios.get(`/api/version/${repoId}/card`)
    content.value = data.content
  } catch (err) {
    console.error('Failed to fetch card:', err)
  } finally {
    loading.value = false
  }
}

const metadata = computed(() => {
  if (!content.value.startsWith('---')) return null
  try {
    const parts = content.value.split('---')
    if (parts.length >= 3) {
      return yaml.load(parts[1])
    }
  } catch (e) {
    return null
  }
  return null
})

const previewHtml = computed(() => {
  let text = content.value
  if (text.startsWith('---')) {
    const parts = text.split('---')
    if (parts.length >= 3) {
      text = parts.slice(2).join('---')
    }
  }
  return marked(text)
})

const saveCard = async () => {
  try {
    await axios.post(`/api/version/${repoId}/card`, { content: content.value })
    alert('Model card saved successfully!')
  } catch (err) {
    alert('Failed to save: ' + err.message)
  }
}

const back = () => router.back()

onMounted(fetchCard)
</script>
