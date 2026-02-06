<script setup>
import { ref, onMounted, onUnmounted } from 'vue';
import { useRoute } from 'vue-router';

const route = useRoute();
const discussionId = route.params.discussionId;
const thread = ref(null);
const newComment = ref('');
const isLoading = ref(true);

const fetchThread = async () => {
  try {
    const res = await fetch(`/api/discussions/thread/${discussionId}`);
    thread.value = await res.json();
  } catch (e) {
    console.error("Failed to fetch thread", e);
  } finally {
    isLoading.value = false;
  }
};

const postComment = async () => {
  if (!newComment.value.trim()) return;
  try {
    const res = await fetch(`/api/discussions/thread/${discussionId}/comment`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content: newComment.value })
    });
    if (res.ok) {
      newComment.value = '';
      await fetchThread();
    }
  } catch (e) {
    alert("Failed to post comment");
  }
};

onMounted(() => {
  fetchThread();
  
  // SSE Listener for real-time updates
  const eventSource = new EventSource('/api/discussions/notifications/sse');
  eventSource.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.type === 'new_comment' && data.data.discussion_id === parseInt(discussionId)) {
        fetchThread();
    }
  };
  onUnmounted(() => eventSource.close());
});
</script>

<template>
  <div class="discussion-thread p-8 max-w-4xl mx-auto">
    <div v-if="isLoading" class="text-center py-20 text-gray-400">
       <svg class="animate-spin h-8 w-8 mx-auto mb-4" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg>
       <span>Loading thread...</span>
    </div>
    
    <div v-else-if="thread">
      <!-- Back Link -->
      <router-link :to="`/repo/${thread.repository_id}/discussions`" class="inline-flex items-center text-xs text-gray-500 hover:text-indigo-600 mb-6 font-bold uppercase tracking-widest">
         <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18"></path></svg>
         Back to List
      </router-link>

      <!-- Thread Header -->
      <div class="mb-8 border-b pb-8">
        <h1 class="text-3xl font-extrabold text-gray-900 mb-4">{{ thread.title }}</h1>
        <div class="flex items-center space-x-3 text-sm">
           <span class="bg-green-100 text-green-700 px-3 py-1 rounded-full font-bold uppercase text-[10px] tracking-widest">{{ thread.status }}</span>
           <span class="text-gray-400">opened by <span class="font-bold text-gray-700">{{ thread.author }}</span></span>
           <span class="text-gray-300">•</span>
           <span class="text-gray-400">{{ thread.comments.length }} comments</span>
        </div>
      </div>

      <!-- Comments List -->
      <div class="space-y-8 mb-12">
        <div v-for="c in thread.comments" :key="c.id" class="comment flex space-x-5">
          <div class="flex-shrink-0">
             <div class="w-12 h-12 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-2xl flex items-center justify-center text-white font-extrabold text-lg shadow-lg">
               {{ c.author[0].toUpperCase() }}
             </div>
          </div>
          <div class="flex-1 bg-white border border-gray-100 rounded-3xl shadow-sm hover:shadow-md transition-shadow p-6">
            <div class="flex justify-between items-center mb-4">
               <span class="font-bold text-gray-900 flex items-center">
                 {{ c.author }}
                 <span v-if="c.author === thread.author" class="ml-2 px-1.5 py-0.5 bg-gray-100 text-gray-500 text-[9px] rounded font-bold uppercase">Author</span>
               </span>
               <span class="text-[10px] text-gray-400 uppercase tracking-widest font-bold">{{ new Date(c.created_at).toLocaleString() }}</span>
            </div>
            <div class="text-gray-700 text-sm leading-relaxed whitespace-pre-wrap prose max-w-none">{{ c.content }}</div>
          </div>
        </div>
      </div>

      <!-- New Comment -->
      <div class="mt-12 pt-10 border-t border-gray-100">
        <h3 class="font-extrabold text-xl mb-6 text-gray-900">Join the discussion</h3>
        <div class="bg-white border border-gray-100 rounded-3xl shadow-sm p-2">
          <textarea 
            v-model="newComment" 
            rows="5" 
            class="w-full p-4 border-none focus:ring-0 outline-none text-sm bg-transparent" 
            placeholder="Write your thoughts here... (Markdown supported)"
          ></textarea>
          <div class="p-2 flex justify-between items-center border-t border-gray-50 mt-2">
             <div class="flex space-x-2 px-2">
                <button title="Markdown help" class="p-1.5 text-gray-400 hover:text-indigo-600 rounded transition-colors">
                  <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                </button>
             </div>
             <button 
               @click="postComment" 
               class="px-8 py-2.5 bg-indigo-600 hover:bg-indigo-700 text-white rounded-2xl font-bold shadow-indigo-200 shadow-lg transition-all transform active:scale-95"
             >
               Post Comment
             </button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.prose { font-family: inherit; }
</style>
