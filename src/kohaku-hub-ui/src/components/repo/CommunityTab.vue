<template>
  <div class="community-tab space-y-6">
    <!-- Header Section -->
    <div class="card">
      <div class="flex items-center justify-between mb-4">
        <div>
          <h2 class="text-2xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
            <div class="i-carbon-chat text-3xl text-blue-500" />
            Community Discussions
          </h2>
          <p class="text-sm text-gray-600 dark:text-gray-400 mt-1">
            Engage with the community, ask questions, and share feedback
          </p>
        </div>
        <button
          class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
          disabled
          title="Coming soon"
        >
          <div class="i-carbon-add" />
          New Discussion
        </button>
      </div>

      <!-- Statistics Row -->
      <div class="grid grid-cols-1 sm:grid-cols-3 gap-4 mt-6">
        <div class="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
          <div class="flex items-center gap-3">
            <div class="i-carbon-forum text-2xl text-blue-600 dark:text-blue-400" />
            <div>
              <div class="text-2xl font-bold text-blue-900 dark:text-blue-100">0</div>
              <div class="text-xs text-blue-700 dark:text-blue-300">Discussions</div>
            </div>
          </div>
        </div>
        <div class="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg p-4">
          <div class="flex items-center gap-3">
            <div class="i-carbon-user-multiple text-2xl text-green-600 dark:text-green-400" />
            <div>
              <div class="text-2xl font-bold text-green-900 dark:text-green-100">0</div>
              <div class="text-xs text-green-700 dark:text-green-300">Contributors</div>
            </div>
          </div>
        </div>
        <div class="bg-purple-50 dark:bg-purple-900/20 border border-purple-200 dark:border-purple-800 rounded-lg p-4">
          <div class="flex items-center gap-3">
            <div class="i-carbon-chat-bot text-2xl text-purple-600 dark:text-purple-400" />
            <div>
              <div class="text-2xl font-bold text-purple-900 dark:text-purple-100">0</div>
              <div class="text-xs text-purple-700 dark:text-purple-300">Comments</div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Discussion Topics -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <!-- Questions -->
      <div class="card hover:shadow-lg transition-shadow">
        <div class="flex items-center gap-2 mb-3">
          <div class="i-carbon-help text-xl text-blue-500" />
          <h3 class="font-semibold text-gray-900 dark:text-white">Questions</h3>
        </div>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-4">
          Ask questions about using this dataset
        </p>
        <div class="text-center py-8 text-gray-400 dark:text-gray-500">
          <div class="i-carbon-chat text-4xl mb-2 inline-block opacity-50" />
          <p class="text-xs">No questions yet</p>
        </div>
      </div>

      <!-- Feature Requests -->
      <div class="card hover:shadow-lg transition-shadow">
        <div class="flex items-center gap-2 mb-3">
          <div class="i-carbon-lightbulb text-xl text-yellow-500" />
          <h3 class="font-semibold text-gray-900 dark:text-white">Feature Requests</h3>
        </div>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-4">
          Suggest improvements or new features
        </p>
        <div class="text-center py-8 text-gray-400 dark:text-gray-500">
          <div class="i-carbon-idea text-4xl mb-2 inline-block opacity-50" />
          <p class="text-xs">No requests yet</p>
        </div>
      </div>

      <!-- Issues -->
      <div class="card hover:shadow-lg transition-shadow">
        <div class="flex items-center gap-2 mb-3">
          <div class="i-carbon-warning text-xl text-red-500" />
          <h3 class="font-semibold text-gray-900 dark:text-white">Issues</h3>
        </div>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-4">
          Report bugs or data quality issues
        </p>
        <div class="text-center py-8 text-gray-400 dark:text-gray-500">
          <div class="i-carbon-debug text-4xl mb-2 inline-block opacity-50" />
          <p class="text-xs">No issues yet</p>
        </div>
      </div>
    </div>

    <!-- Coming Soon Notice -->
    <div class="card bg-gradient-to-r from-blue-50 to-purple-50 dark:from-blue-900/20 dark:to-purple-900/20 border-2 border-dashed border-blue-300 dark:border-blue-700">
      <div class="flex items-start gap-4">
        <div class="i-carbon-rocket text-4xl text-blue-600 dark:text-blue-400 flex-shrink-0" />
        <div class="flex-1">
          <h3 class="text-lg font-bold text-gray-900 dark:text-white mb-2">
            Community Features Coming Soon!
          </h3>
          <p class="text-sm text-gray-700 dark:text-gray-300 mb-4">
            We're building a full-featured discussion system to help you collaborate with the community. Here's what's planned:
          </p>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
              <div class="i-carbon-checkmark-outline text-green-600 dark:text-green-400" />
              <span>Threaded discussions</span>
            </div>
            <div class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
              <div class="i-carbon-checkmark-outline text-green-600 dark:text-green-400" />
              <span>Markdown support</span>
            </div>
            <div class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
              <div class="i-carbon-checkmark-outline text-green-600 dark:text-green-400" />
              <span>Notifications</span>
            </div>
            <div class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
              <div class="i-carbon-checkmark-outline text-green-600 dark:text-green-400" />
              <span>Vote and reaction system</span>
            </div>
            <div class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
              <div class="i-carbon-checkmark-outline text-green-600 dark:text-green-400" />
              <span>Category filtering</span>
            </div>
            <div class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
              <div class="i-carbon-checkmark-outline text-green-600 dark:text-green-400" />
              <span>User mentions</span>
            </div>
          </div>
          <div class="mt-4 pt-4 border-t border-blue-200 dark:border-blue-800">
            <p class="text-xs text-gray-600 dark:text-gray-400">
              Want to contribute or stay updated? Follow development on 
              <a href="#" class="text-blue-600 dark:text-blue-400 hover:underline">GitHub</a>
            </p>
          </div>
        </div>
      </div>
    </div>

    <!-- Temporary Alternatives -->
    <div class="card">
      <h3 class="font-semibold text-gray-900 dark:text-white mb-3 flex items-center gap-2">
        <div class="i-carbon-information" />
        Meanwhile, you can:
      </h3>
      <div class="space-y-2 text-sm">
        <a href="#" class="flex items-center gap-2 p-2 hover:bg-gray-100 dark:hover:bg-gray-800 rounded transition-colors text-blue-600 dark:text-blue-400">
          <div class="i-carbon-email" />
          <span>Contact the repository owner directly</span>
        </a>
        <a href="#" class="flex items-center gap-2 p-2 hover:bg-gray-100 dark:hover:bg-gray-800 rounded transition-colors text-blue-600 dark:text-blue-400">
          <div class="i-carbon-logo-github" />
          <span>Open an issue on the source repository</span>
        </a>
        <a href="#" class="flex items-center gap-2 p-2 hover:bg-gray-100 dark:hover:bg-gray-800 rounded transition-colors text-blue-600 dark:text-blue-400">
          <div class="i-carbon-forum" />
          <span>Join our community forum</span>
        </a>
      </div>
    </div>
  </div>
</template>

<script setup>
defineProps({
  namespace: String,
  name: String,
  repoType: String
})
</script>
