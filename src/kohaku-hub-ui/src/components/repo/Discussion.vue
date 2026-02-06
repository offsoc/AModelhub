<script setup>
import { ref, onMounted } from 'vue';

const props = defineProps({
  repoId: {
    type: String,
    required: true
  }
});

const discussions = ref([]);
const isLoading = ref(false);
const showNewThreadModal = ref(false);
const newThread = ref({ title: '', comment: '' });

const fetchDiscussions = async () => {
  isLoading.value = true;
  try {
    const res = await fetch(`/api/discussions/${props.repoId}`);
    discussions.value = await res.json();
  } catch (e) {
    console.error("Failed to fetch discussions", e);
  } finally {
    isLoading.value = false;
  }
};

const createThread = async () => {
  try {
    const res = await fetch(`/api/discussions/${props.repoId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(newThread.value)
    });
    if (res.ok) {
       showNewThreadModal.value = false;
       await fetchDiscussions();
    }
  } catch (e) {
    alert("Failed to create thread");
  }
};

onMounted(fetchDiscussions);
</script>

<template>
  <div class="discussions-tab p-6">
    <div class="flex justify-between items-center mb-6">
      <h2 class="text-xl font-bold flex items-center space-x-2">
        <svg class="w-6 h-6 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012-2v8a2 2 0 01-2 2h-5l-5 5v-5z"></path></svg>
        <span>Discussions</span>
      </h2>
      <button 
        @click="showNewThreadModal = true"
        class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-sm font-semibold transition-colors"
      >
        New Discussion
      </button>
    </div>

    <!-- Empty State -->
    <div v-if="discussions.length === 0 && !isLoading" class="text-center py-20 bg-gray-50 rounded-xl border-2 border-dashed border-gray-100">
       <p class="text-gray-400">No discussions yet. Start the first one!</p>
    </div>

    <!-- List -->
    <div v-else class="space-y-4">
      <router-link 
        v-for="d in discussions" 
        :key="d.id"
        :to="`/repo/${repoId}/discussions/${d.id}`"
        class="block bg-white border border-gray-100 p-4 rounded-xl hover:shadow-md transition-shadow"
      >
        <div class="flex justify-between items-start">
          <div class="flex items-center space-x-3">
             <span :class="['px-2 py-0.5 text-[10px] font-bold rounded uppercase tracking-wider', d.status === 'open' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700']">
               {{ d.status }}
             </span>
             <h3 class="font-bold text-gray-900">{{ d.title }}</h3>
          </div>
          <span class="text-xs text-gray-400 flex items-center space-x-1">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 8h10M7 12h4m1 8l-4-4H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-3l-4 4z"></path></svg>
            <span>{{ d.comment_count }}</span>
          </span>
        </div>
        <div class="mt-2 text-xs text-gray-500">
          opened by <span class="font-medium text-gray-700">{{ d.author }}</span> on {{ new Date(d.created_at).toLocaleDateString() }}
        </div>
      </router-link>
    </div>

    <!-- Modal -->
    <div v-if="showNewThreadModal" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
       <div class="bg-white p-6 rounded-2xl w-full max-w-lg shadow-2xl">
          <h3 class="text-lg font-bold mb-4">New Discussion</h3>
          <input v-model="newThread.title" type="text" placeholder="Title" class="w-full p-3 border rounded-lg mb-4 text-sm focus:ring-2 focus:ring-indigo-200 outline-none">
          <textarea v-model="newThread.comment" placeholder="Write your first comment..." rows="5" class="w-full p-3 border rounded-lg mb-4 text-sm focus:ring-2 focus:ring-indigo-200 outline-none"></textarea>
          <div class="flex justify-end space-x-3">
             <button @click="showNewThreadModal = false" class="px-4 py-2 text-gray-500 font-bold">Cancel</button>
             <button @click="createThread" class="px-6 py-2 bg-indigo-600 text-white rounded-lg font-bold">Post</button>
          </div>
       </div>
    </div>
  </div>
</template>
